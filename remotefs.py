from cubic_sdk.cubic import Item as SDK_Node, Cubic as CubicServer
from node import Node
import json
from encryption import Encryption
import logging


class RemoteFS:
    def __init__(self, server: CubicServer, key):
        self.server = server
        self.crypto = Encryption(key)
        self.clear()

    def clear(self):
        self.dict = {}
        self.all_block_hashes = set()

    def generate_dict(self, items):
        for item in items:
            path = self.crypto.decrypt(item.path).decode('utf8', errors='surrogateescape')
            meta = json.loads(self.crypto.decrypt(item.meta).decode())
            mode = meta['mode']
            mtime = meta['mtime']
            is_dir = path.endswith('/')
            n = Node(is_dir=is_dir, mode=mode, mtime=mtime)
            if is_dir:
                path = path[:-1]
            else:
                n.size = meta['size']
                n.block_hashes = item.blocks
                self.all_block_hashes.update(n.block_hashes)
            self.dict[path] = n

    def fetch_remote(self):
        logging.info('Downloading remote file list')
        items = list(self.server.get_tree())
        self.clear()
        logging.info('Parsing remote file list')
        self.generate_dict(items)
        logging.info('%s items in total', len(self.dict))

    def check_hashes(self, hashes):
        hashes = list(hashes)
        logging.info('Checking remote existing blocks')
        exists = [hash for hash, exist in zip(hashes, self.server.bulk_head_block(hashes)) if exist]
        logging.info('%s of %s blocks exists', len(exists), len(hashes))
        return exists

    def update_remote(self, *, add, remove):
        logging.info('Updating directory tree')
        remove_list = []
        for path in remove:
            remove_list.append(self.crypto.encrypt(
                (path + ('/' if self.dict[path].is_dir else '')).encode('utf8', errors='surrogateescape')))
        add_list = []
        for path, node in add.items():
            if node.is_dir:
                add_list.append(SDK_Node(
                    self.crypto.encrypt((path + '/').encode('utf8', errors='surrogateescape')),
                    self.crypto.encrypt(json.dumps({'mode': node.mode, 'mtime': node.mtime}).encode()),
                    [],
                ))
            else:
                add_list.append(SDK_Node(
                    self.crypto.encrypt(path.encode('utf8', errors='surrogateescape')),
                    self.crypto.encrypt(
                        json.dumps({'mode': node.mode, 'mtime': node.mtime, 'size': node.size}).encode()),
                    node.block_hashes,
                ))
        self.server.post_tree(put_items=add_list, delete_paths=remove_list)
        logging.info('Directory tree updated')

    def put_blocks(self, blocks):
        self.put_encrypted_blocks([self.crypto.encrypt(block) for block in blocks])

    def put_encrypted_blocks(self, blocks):
        logging.info('Uploading %s blocks, total size %s bytes', len(blocks), sum(len(b) for b in blocks))
        self.server.bulk_post_block(blocks)

    def get_block(self, hash):
        return self.crypto.decrypt(self.server.get_block(hash))
