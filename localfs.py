from node import Node
import os
import logging
import stat


class LocalFS:
    def __init__(self, base_path):
        self.base_path = base_path
        self.clear()

    def clear(self):
        self.dict = {}

    def realpath(self, path):
        return os.path.join(self.base_path, path)

    def generate_dict(self):
        logging.info('Scanning local file list')
        self.clear()
        for root, dirs, files in os.walk(self.base_path, followlinks=True):
            dir_path = os.path.relpath(root, self.base_path)
            if dir_path == '.':
                dir_path = ''
            try:
                self.dict[dir_path] = self.get_dir_node(dir_path)
            except OSError as e:
                logging.exception(e)
                continue
            for file in files:
                file_path = os.path.join(dir_path, file)
                try:
                    self.dict[file_path] = self.get_file_node(file_path)
                except OSError as e:
                    logging.exception(e)
                    continue
        logging.info('%s items in total', len(self.dict))

    def get_file_node(self, path):
        st = os.stat(self.realpath(path))
        if not stat.S_ISREG(st.st_mode):
            raise OSError('Not regular file: %s' % path)
        n = Node(is_dir=False, mode=st.st_mode, mtime=st.st_mtime)
        n.size = st.st_size
        return n

    def get_dir_node(self, path):
        st = os.stat(self.realpath(path))
        return Node(is_dir=True, mode=st.st_mode, mtime=st.st_mtime)
