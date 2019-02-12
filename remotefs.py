from cubic_sdk.cubic import Item as SDK_Node, Cubic as CubicServer
from node import Node
import json


class RemoteFS:
    def __init__(self, server: CubicServer):
        self.server = server
        self.clear()

    def clear(self):
        self.dict = {}

    def generate_dict(self, items):
        if items is None:
            return
        for item in items:
            path = item.path.decode()
            meta = json.loads(item.meta.decode())
            mode = meta['mode']
            mtime = meta['mtime']
            is_dir = path.endswith('/')
            n = Node(is_dir=is_dir, mode=mode, mtime=mtime)
            if is_dir:
                path = path[:-1]
            else:
                n.size = meta['size']
                n.block_hashes = item.blocks
            self.dict[path] = n

    def fetch_remote(self):
        items = self.server.get_tree()
        self.clear()
        self.generate_dict(items)

    def check_hashes(self, hashes):
        return [hash for hash, exist in zip(hashes, self.server.bulk_head_block(hashes)) if exist]

    def update_remote(self, *, add, remove):
        remove_list = []
        for path in remove:
            remove_list.append((path + ('/' if self.dict[path].is_dir else '')).encode())
        add_list = []
        for path, node in add.items():
            if node.is_dir:
                add_list.append(SDK_Node(
                    (path + '/').encode(),
                    json.dumps({'mode': node.mode, 'mtime': node.mtime}).encode(),
                    [],
                ))
            else:
                add_list.append(SDK_Node(
                    path.encode(),
                    json.dumps({'mode': node.mode, 'mtime': node.mtime, 'size': node.size}).encode(),
                    node.block_hashes,
                ))
        self.server.post_tree(put_items=add_list, delete_paths=remove_list)

    def put_blocks(self, blocks):
        self.server.bulk_post_block(blocks)

    def get_blocks(self, hashes):
        return self.server.bulk_get_block(hashes)

    def get_block(self, hash):
        return self.server.get_block(hash)
