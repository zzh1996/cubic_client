#!/usr/bin/env python3

import coloredlogs
import sys
import logging
import config
from cubic_sdk.cubic import Cubic as CubicServer
from localfs import LocalFS
from remotefs import RemoteFS
import getpass
from utils import size


def generate_diff(base_dict, target_dict):
    base_set = set(base_dict.keys())
    target_set = set(target_dict.keys())
    common_items = base_set & target_set
    deleted_items = base_set - target_set
    new_items = target_set - base_set
    for path in common_items:
        if base_dict[path] != target_dict[path]:
            deleted_items.add(path)
            new_items.add(path)
    return deleted_items, new_items


def sync(localfs, remotefs):
    logging.info('Fetching remote file list')
    remotefs.fetch_remote()
    logging.info('%s items in total', len(remotefs.dict))
    logging.info('Scanning local file list')
    localfs.generate_dict()
    logging.info('%s items in total', len(localfs.dict))
    logging.info('Calculating changes')
    deleted_items, new_items = generate_diff(remotefs.dict, localfs.dict)
    logging.info('%s items to be removed, %s items to be uploaded', len(deleted_items), len(new_items))
    if not deleted_items and not new_items:
        return

    logging.info('Counting files')
    total_files = sum(1 for path in new_items if not localfs.dict[path].is_dir)
    total_size = sum(localfs.dict[path].size for path in new_items if not localfs.dict[path].is_dir)

    new_items = sorted(new_items)
    all_block_hashes = set()
    error_items = []
    processed_files = 0
    processed_size = 0
    logging.info('Scanning local files')
    for path in new_items:
        if not localfs.dict[path].is_dir:
            processed_files += 1
            processed_size += localfs.dict[path].size
            logging.info('Scanning file [%s/%s][%s/%s][%s] %s', processed_files, total_files,
                         size(processed_size), size(total_size), size(localfs.dict[path].size), path)
            try:
                localfs.generate_block_hashes(path)
            except OSError as e:
                logging.exception(e)
                error_items.append(path)
                continue
            for block_hash in localfs.dict[path].block_hashes:
                all_block_hashes.add(block_hash)
    new_items = [item for item in new_items if item not in error_items]
    while True:
        logging.info('Checking remote existing blocks')
        nonexists = all_block_hashes - set(remotefs.check_hashes(list(all_block_hashes)))
        logging.info('%s blocks exists, %s blocks to be uploaded',
                     len(all_block_hashes - nonexists),
                     len(nonexists),
                     )
        if not nonexists:
            logging.info('All blocks uploaded, updating directory tree')
            add = {path: localfs.dict[path] for path in new_items}
            if remotefs.update_remote(add=add, remove=deleted_items):
                logging.info('Uploading done')
            else:
                logging.error('Updating directory tree failed')
            return

        buffer = []
        for path in new_items:
            node = localfs.dict[path]
            if not node.is_dir:
                f = None
                try:
                    for i, block_hash in enumerate(node.block_hashes):
                        if block_hash in nonexists:
                            if f is None:
                                logging.info('Reading File %s', path)
                                f = open(localfs.realpath(path), 'rb')
                            f.seek(config.block_size * i, 0)
                            block_data = f.read(config.block_size)
                            buffer.append(block_data)
                            nonexists.remove(block_hash)
                            total_size = sum(len(b) for b in buffer)
                            if total_size >= config.upload_max_size:
                                logging.info(
                                    'Uploading %s blocks, total size %s bytes',
                                    len(buffer),
                                    total_size,
                                )
                                remotefs.put_blocks(buffer)
                                buffer = []
                    if f is not None:
                        f.close()
                except OSError as e:
                    logging.exception(e)
                    # TODO: path in new_items should be deleted
                    if f is not None:
                        f.close()
        if buffer:
            logging.info('Uploading the last blocks')
            remotefs.put_blocks(buffer)


if __name__ == '__main__':
    coloredlogs.install(level=config.log_level, fmt=config.log_format)
    server = CubicServer(sys.argv[1], sys.argv[2])
    local_dir = sys.argv[3]
    if len(sys.argv) >= 5:
        key = sys.argv[4]
    else:
        pw1 = getpass.getpass()
        pw2 = getpass.getpass()
        if pw1 == pw2:
            key = pw1
        else:
            exit(-1)
    localfs = LocalFS(local_dir, key)
    remotefs = RemoteFS(server, key)
    while sync(localfs, remotefs):
        pass
