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
    logging.info('Calculating changes')
    base_set = set(base_dict.keys())
    target_set = set(target_dict.keys())
    common_items = base_set & target_set
    deleted_items = base_set - target_set
    new_items = target_set - base_set
    for path in common_items:
        if base_dict[path] != target_dict[path]:
            deleted_items.add(path)
            new_items.add(path)
    logging.info('%s items to be removed, %s items to be uploaded', len(deleted_items), len(new_items))
    return sorted(deleted_items), sorted(new_items)


class UploadBuffer:
    def __init__(self, remotefs: RemoteFS, max_size=config.upload_max_size):
        self.remotefs = remotefs
        self.max_size = max_size
        self.blocks = {}

    def size(self):
        return sum(len(b) for b in self.blocks.values())

    def check_dup_local(self, block_hash):
        return block_hash in self.blocks or block_hash in remotefs.all_block_hashes

    def upload_all(self):
        if not self.blocks:
            return
        self.remotefs.all_block_hashes.update(self.blocks)
        exists = self.remotefs.check_hashes(self.blocks)
        for block_hash in exists:
            del self.blocks[block_hash]
        if self.blocks:
            self.remotefs.put_encrypted_blocks(self.blocks.values())
            self.blocks.clear()

    def put_block(self, block_data):
        encrypted_block_data = self.remotefs.crypto.encrypt(block_data)
        block_hash = config.hash_algo(encrypted_block_data)
        if not self.check_dup_local(block_hash):
            self.blocks[block_hash] = encrypted_block_data
            if self.size() >= self.max_size:
                self.upload_all()
        return block_hash


def sync(localfs, remotefs):
    remotefs.fetch_remote()
    if config.check_integrity:
        if len(remotefs.check_hashes(remotefs.all_block_hashes)) != len(remotefs.all_block_hashes):
            logging.error('Some of the remote blocks are missing')
            return
    localfs.generate_dict()
    deleted_items, new_items = generate_diff(remotefs.dict, localfs.dict)
    if not deleted_items and not new_items:
        logging.info('Already up to date')
        return

    total_files = sum(1 for path in new_items if not localfs.dict[path].is_dir)
    total_size = sum(localfs.dict[path].size for path in new_items if not localfs.dict[path].is_dir)

    error_items = set()
    processed_files = 0
    processed_size = 0
    buffer = UploadBuffer(remotefs)

    logging.info('Start uploading files')
    for path in new_items:
        item = localfs.dict[path]
        if not item.is_dir:
            processed_files += 1
            processed_size += item.size
            logging.info('Processing file [%s/%s][%s/%s][%s] %s', processed_files, total_files,
                         size(processed_size), size(total_size), size(item.size), path)
            try:
                if localfs.get_file_node(path) != item:
                    logging.warning('File changed while syncing, skipping')
                    error_items.add(path)
                with open(localfs.realpath(path), 'rb') as f:
                    item.block_hashes = []
                    for block_data in iter(lambda: f.read(config.block_size), b''):
                        block_hash = buffer.put_block(block_data)
                        item.block_hashes.append(block_hash)
            except OSError as e:
                logging.exception(e)
                error_items.add(path)
    buffer.upload_all()
    logging.info('All blocks uploaded')
    add = {path: localfs.dict[path] for path in new_items if path not in error_items}
    remotefs.update_remote(add=add, remove=deleted_items)
    logging.info('All done')


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
    localfs = LocalFS(local_dir)
    remotefs = RemoteFS(server, key)
    sync(localfs, remotefs)
