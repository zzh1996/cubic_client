#!/usr/bin/env python3
import logging
import sys
import config
import os
import time

from remotefs import RemoteFS
from errno import ENOENT
from cubic_sdk.cubic import Cubic as CubicServer
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn


class BlockCache:
    def __init__(self, remotefs, max_size=16 * 1024 * 1024):
        self.cache = {}
        self.remotefs = remotefs
        self.max_size = max_size

    def _get_block(self, block_hash):
        return self.remotefs.get_block(block_hash)

    def get(self, block_hash):
        if block_hash not in self.cache:
            logging.debug('Cache miss')
            data = self._get_block(block_hash)
            while sum(len(block) for block, _ in self.cache.values()) + len(data) > self.max_size:
                oldest = time.time()
                for h, (block, t) in self.cache.items():
                    if t < oldest:
                        oldest = t
                        oldest_hash = h
                del self.cache[oldest_hash]
            logging.debug('Cache block count = %s, total size = %s',
                          len(self.cache), sum(len(block) for block, _ in self.cache.values()))
        else:
            logging.debug('Cache hit')
            data, _ = self.cache[block_hash]
        self.cache[block_hash] = data, time.time()
        return data


class CubicFS(LoggingMixIn, Operations):
    def __init__(self, user, token, key):
        self.remotefs = RemoteFS(CubicServer(user, token), key)
        self.remotefs.fetch_remote()
        self.block_cache = BlockCache(self.remotefs)

    def getattr(self, path, fh=None):
        path = path[1:]
        if path in self.remotefs.dict:
            item = self.remotefs.dict[path]
            st_mtime = item.mtime
            st_atime = item.mtime
            st_ctime = item.mtime
            st_uid = os.getuid()
            st_gid = os.getgid()
            st_mode = item.mode
            st_size = 0 if item.is_dir else item.size
            st_nlink = 1
            return dict(st_mtime=st_mtime, st_atime=st_atime, st_ctime=st_ctime, st_uid=st_uid,
                        st_gid=st_gid, st_mode=st_mode, st_size=st_size, st_nlink=st_nlink)
        else:
            raise FuseOSError(ENOENT)

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
                if p and '/' not in p:
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
    logging.basicConfig(level=logging.DEBUG)
    if len(sys.argv) >= 5:
        key = sys.argv[4]
    else:
        key = None
    fuse = FUSE(CubicFS(sys.argv[1], sys.argv[2], key), sys.argv[3], foreground=True)
