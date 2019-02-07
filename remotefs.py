from cubic_sdk.cubic import Node as SDK_Node, CubicServer
from node import Node
import json


class RemoteFS:
    def __init__(self, server: CubicServer):
        self.server = server
        self.clear()

    def clear(self):
        self.dict = {}

    def generate_dict(self, sdk_root: SDK_Node):
        path = sdk_root.path.decode()
        while path.startswith("/"):
            path = path[1:]
        if path:
            meta = json.loads(sdk_root.meta.decode())
            mode = meta['mode']
            mtime = meta['mtime']
            n = Node(is_dir=sdk_root.is_dir, mode=mode, mtime=mtime)
            if not n.is_dir:
                n.size = sdk_root.size
                n.block_hashes = sdk_root.blocks
            self.dict[path] = n
        for c in sdk_root.children:
            self.generate_dict(c)

    def fetch_remote(self):
        sdk_root = self.server.get_tree(SDK_Node(b'/'))
        self.clear()
        self.generate_dict(sdk_root)

    def update_remote(self, *, add, remove):
        remove_list = [SDK_Node(path.encode(), is_dir=self.dict[path].is_dir) for path in remove]
        remove_list.sort(key=lambda n: n.path)
        add_list = []
        for path, node in add.items():
            if node.is_dir:
                add_list.append(SDK_Node(
                    path.encode(),
                    is_dir=True,
                    meta=json.dumps({'mode': node.mode, 'mtime': node.mtime}).encode(),
                ))
            else:
                add_list.append(SDK_Node(
                    path.encode(),
                    is_dir=False,
                    meta=json.dumps({'mode': node.mode, 'mtime': node.mtime}).encode(),
                    size=node.size,
                    blocks=node.block_hashes
                ))
        self.server.update_tree(add=add_list, remove=remove_list)
