from node import Node
import os
import config
from encryption import Encryption
import logging
import stat


class LocalFS:
    def __init__(self, base_path, key):
        self.base_path = base_path
        self.crypto = Encryption(key)
        self.clear()

    def clear(self):
        self.dict = {}

    def realpath(self, path):
        return os.path.join(self.base_path, path)

    def generate_dict(self):
        self.clear()
        for root, dirs, files in os.walk(self.base_path, followlinks=True):
            dir_path = os.path.relpath(root, self.base_path)
            if dir_path == '.':
                dir_path = ''
            try:
                st = os.stat(self.realpath(dir_path))
            except OSError as e:
                logging.exception(e)
                continue
            self.dict[dir_path] = Node(is_dir=True, mode=st.st_mode, mtime=st.st_mtime)
            for file in files:
                file_path = os.path.join(dir_path, file)
                try:
                    st = os.stat(self.realpath(file_path))
                except OSError as e:
                    logging.exception(e)
                    continue
                if not stat.S_ISREG(st.st_mode):
                    logging.warning('Not regular file: %s' % file_path)
                    continue
                n = Node(is_dir=False, mode=st.st_mode, mtime=st.st_mtime)
                n.size = st.st_size
                self.dict[file_path] = n

    def generate_block_hashes(self, path):
        assert path in self.dict and not self.dict[path].is_dir
        with open(self.realpath(path), 'rb') as f:
            self.dict[path].block_hashes = []
            for block in iter(lambda: f.read(config.block_size), b''):
                self.dict[path].block_hashes.append(config.hash_algo(self.crypto.encrypt(block)))
