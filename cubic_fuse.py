#!/usr/bin/env python3
import logging
import sys
import config
import os
import time

from remotefs import RemoteFS
from cubic_sdk.cubic import CubicServer
from errno import ENOENT

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class BlockCache:
    def __init__(self, server, max_size=16 * 1024 * 1024):
        self.cache = {}
        self.server = server
        self.max_size = max_size

    def _get_block(self, block_hash):
        data = self.server.get_block([block_hash])[block_hash]
        assert block_hash == config.hash_algo(data)
        return data

    def get(self, block_hash):
        if block_hash not in self.cache:
            logging.info('Cache miss')
            data = self._get_block(block_hash)
            while sum(len(block) for block, _ in self.cache.values()) + len(data) > self.max_size:
                oldest = time.time()
                for h, (block, t) in self.cache.items():
                    if t < oldest:
                        oldest = t
                        oldest_hash = h
                del self.cache[oldest_hash]
            logging.info('Cache block count = %s, total size = %s',
                         len(self.cache), sum(len(block) for block, _ in self.cache.values()))
        else:
            logging.info('Cache hit')
            data, _ = self.cache[block_hash]
        self.cache[block_hash] = data, time.time()
        return data


class CubicFS(LoggingMixIn, Operations):
    def __init__(self):
        root = os.stat(sys.argv[1])
        self.root_attr = {'st_size': root.st_size, 'st_mtime': root.st_mtime, 'st_mode': root.st_mode}
        self.remotefs = RemoteFS(CubicServer(config.server_addr))
        self.remotefs.fetch_remote()
        self.block_cache = BlockCache(self.remotefs.server)

    def getattr(self, path, fh=None):
        path = path[1:]
        if path:
            if path in self.remotefs.dict:
                item = self.remotefs.dict[path]
                if item.is_dir:
                    return {'st_size': 0, 'st_mtime': item.mtime, 'st_mode': item.mode}
                else:
                    return {'st_size': item.size, 'st_mtime': item.mtime, 'st_mode': item.mode}
            else:
                raise FuseOSError(ENOENT)
        else:
            return self.root_attr

    def read(self, path, size, offset, fh):
        path = path[1:]
        if path in self.remotefs.dict and not self.remotefs.dict[path].is_dir:
            item = self.remotefs.dict[path]
            data = b''
            start_block = offset // config.block_size
            end_block = (offset + size - 1) // config.block_size
            for block_hash in item.block_hashes[start_block:end_block + 1]:
                block = self.block_cache.get(block_hash)
                data += block
            return data[offset % config.block_size:offset % config.block_size + size]
        else:
            raise FuseOSError(ENOENT)

    def readdir(self, path, fh):
        items = ['.', '..']
        path = path[1:]
        if path == '':
            for p in self.remotefs.dict:
                if '/' not in p:
                    items.append(p)
            return items
        elif path in self.remotefs.dict and self.remotefs.dict[path].is_dir:
            for p in self.remotefs.dict:
                if p.startswith(path + '/') and '/' not in p[len(path) + 1:]:
                    items.append(p[len(path) + 1:])
            return items
        else:
            raise FuseOSError(ENOENT)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    fuse = FUSE(CubicFS(), sys.argv[1], foreground=True)
