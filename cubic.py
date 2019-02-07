#!/usr/bin/env python3
import config
import os
import logging
import sys
import copy
import coloredlogs
from cubic_sdk.cubic import CubicServer, hash, Node
import json
import shutil


class DummyServer:
    def __init__(self):
        self.blocks = set()
        self.data = {}

    def put_blocks(self, blockdata_list):
        logging.debug("Put blocks: %s", [len(b) for b in blockdata_list])
        for block in blockdata_list:
            self.blocks.add(config.hash_algo(block))

    def update(self, updates):
        logging.debug("Updates: %s", updates)
        new_data = copy.deepcopy(self.data)
        nonexists = []
        for u in updates:
            if u["method"] == "DELETE":
                del new_data[u["path"]]
            elif u["method"] == "PUT":
                if u["path"].endswith("/"):
                    d = Dir()
                    d.meta = u["meta"]
                    new_data[u["path"]] = d
                else:
                    f = File()
                    f.meta = u["meta"]
                    f.blocks = u["blocks"]
                    for block in f.blocks:
                        if block not in self.blocks:
                            nonexists.append(block)
                    new_data[u["path"]] = f
        if not nonexists:
            self.data = copy.deepcopy(new_data)
        return nonexists

    def getlist(self):
        return self.data


class File:
    def __init__(self):
        self.meta = {}
        self.hash_list = []

    def __repr__(self):
        return str({"meta": self.meta, "hash_list": self.hash_list})


class Dir:
    def __init__(self):
        self.meta = {}

    def __repr__(self):
        return str({"meta": self.meta})


class Cache:
    def __init__(self, base_path):
        self.base_path = base_path
        self.data = {}

    def clear(self):
        self.data = {}

    def update_file(self, path):
        f = File()
        st = os.stat(path)
        f.meta["mode"] = st.st_mode
        f.meta["mtime"] = st.st_mtime
        # TODO: handle exceptions when opening and reading file
        with open(path, "rb") as fd:
            for block in iter(lambda: fd.read(config.block_size), b""):
                f.hash_list.append(config.hash_algo(block))
        rel_path = os.path.relpath(path, self.base_path)
        self.data[rel_path] = f

    def update_dir(self, path):
        d = Dir()
        st = os.stat(path)
        d.meta["mode"] = st.st_mode
        d.meta["mtime"] = st.st_mtime
        rel_path = os.path.relpath(path, self.base_path)
        self.data[rel_path] = d

    def rebuild_tree(self):
        self.clear()
        for root, dirs, files in os.walk(self.base_path, followlinks=True):
            if os.path.relpath(root, self.base_path) != ".":
                logging.info("Scanning Dir: %s", os.path.relpath(root, self.base_path))
                self.update_dir(root)
            for file in files:
                logging.info(
                    "Scanning File: %s",
                    os.path.relpath(os.path.join(root, file), self.base_path),
                )
                self.update_file(os.path.join(root, file))


def sync_dummy(cache):
    server = DummyServer()
    updates = []
    for path, item in cache.data.items():
        if isinstance(item, File):
            updates.append(
                {
                    "method": "PUT",
                    "path": path,
                    "meta": item.meta,
                    "blocks": item.hash_list,
                }
            )
        else:
            if not path.endswith("/"):
                path += "/"
            updates.append({"method": "PUT", "path": path, "meta": item.meta})
    while True:
        nonexists = server.update(updates)
        if not nonexists:
            break
        buffer = []
        for path, item in cache.data.items():
            if isinstance(item, File):
                fd = None
                for i, block in enumerate(item.hash_list):
                    if block in nonexists:
                        if fd is None:
                            fd = open(os.path.join(cache.base_path, path), "rb")
                        fd.seek(config.block_size * i, 0)
                        block_data = fd.read(config.block_size)
                        buffer.append(block_data)
                        if sum(len(b) for b in buffer) >= config.block_size:
                            server.put_blocks(buffer)
                            buffer = []
                if fd is not None:
                    fd.close()
        if buffer:
            server.put_blocks(buffer)


def sync(cache, server):
    logging.info("Calculating updates")
    updates = []
    all_hash = []
    for path, item in cache.data.items():
        if isinstance(item, File):
            updates.append(
                Node(
                    path.encode(),
                    is_dir=False,
                    meta=json.dumps(item.meta).encode(),
                    blocks=item.hash_list,
                )
            )
            all_hash.extend(item.hash_list)
        else:
            updates.append(
                Node(path.encode(), is_dir=True, meta=json.dumps(item.meta).encode())
            )
    while True:
        logging.info("Checking remote existing blocks")
        nonexists = list(set(all_hash) - set(server.check_hash(all_hash)))
        if not nonexists:
            logging.info("All blocks uploaded, updating directory tree")
            # server.update_tree(add=[], remove=server.get_tree(Node(b"/")).children)
            # for n in server.get_tree(Node(b"/")).children:
            #     server.update_tree(add=[], remove=[n])
            # server.update_tree(add=[], remove=[Node(b"/")])
            logging.info("Updating directory tree")
            server.update_tree(add=updates, remove=[Node(b"/")])
            logging.info("Uploading done")
            break
        buffer = {}
        for path, item in cache.data.items():
            if isinstance(item, File):
                fd = None
                for i, block in enumerate(item.hash_list):
                    if block in nonexists:
                        if fd is None:
                            logging.info("Reading File %s", path)
                            fd = open(os.path.join(cache.base_path, path), "rb")
                        fd.seek(config.block_size * i, 0)
                        block_data = fd.read(config.block_size)
                        buffer[config.hash_algo(block_data)] = block_data
                        total_size = sum(len(b) for b in buffer.values())
                        if total_size >= config.block_size:
                            logging.info(
                                "Uploading blocks size = %s, hash = %s",
                                total_size,
                                list(buffer.keys()),
                            )
                            server.put_block(buffer)
                            buffer = {}
                if fd is not None:
                    fd.close()
        if buffer:
            total_size = sum(len(b) for b in buffer.values())
            logging.info(
                "Uploading blocks size = %s, hash = %s", total_size, list(buffer.keys())
            )
            server.put_block(buffer)


def print_node(n):
    if n.is_dir:
        print(n.path.decode(), n.meta.decode())
        for c in n.children:
            print_node(c)
    else:
        print(n.path.decode(), n.meta.decode(), n.blocks)


def print_remote_tree(server):
    print_node(server.get_tree(Node(b"/")))


def download_node(server, node, target):
    path = node.path.decode()
    while path.startswith("/"):
        path = path[1:]
    absolute_path = os.path.join(target, path)
    try:
        meta = json.loads(node.meta)
    except:
        if path:
            logging.warning("Metadata for %s is not valid", path)
        meta = {}
    if node.is_dir:
        if path:
            logging.info("Downloading Dir %s", path)
        if "mode" in meta:
            os.makedirs(absolute_path, meta["mode"], exist_ok=True)
        else:
            os.makedirs(absolute_path, exist_ok=True)
        for c in node.children:
            download_node(server, c, target)
        if "mtime" in meta:
            os.utime(absolute_path, (meta["mtime"], meta["mtime"]))
    else:
        logging.info("Downloading File %s", path)
        with open(absolute_path, "wb") as f:
            for h in node.blocks:
                logging.info("Downloading block %s", h)
                f.write(server.get_block([h])[h])
        if "mode" in meta:
            os.chmod(absolute_path, meta["mode"])
        if "mtime" in meta:
            os.utime(absolute_path, (meta["mtime"], meta["mtime"]))


def download_tree(server, target):
    logging.info("Removing existing files in target directory")
    shutil.rmtree(target)
    logging.info("Downloading directory tree")
    download_node(server, server.get_tree(Node(b"/")), target)
    logging.info("Downloading done")


def main():
    coloredlogs.install(
        level=logging.INFO,
        fmt="%(asctime)s.%(msecs)03d %(levelname)s %(message)s",
    )
    server = CubicServer("http://mirrors.asia:8000/")
    if sys.argv[1] == "upload":
        c = Cache(sys.argv[2])
        c.rebuild_tree()
        sync(c, server)
    elif sys.argv[1] == "print":
        print_remote_tree(server)
    elif sys.argv[1] == "download":
        download_tree(server, sys.argv[2])
    elif sys.argv[1] == "reset":
        server.reset()


if __name__ == "__main__":
    main()
