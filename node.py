class Node:
    def __init__(self, *, is_dir, mode, mtime):
        self.is_dir = is_dir
        self.mode = mode
        self.mtime = mtime
        if not self.is_dir:
            self.size = None
            self.block_hashes = None

    def __eq__(self, other):
        if self.is_dir != other.is_dir:
            return False
        if self.mode != other.mode:
            return False
        if self.mtime != other.mtime:
            return False
        if not self.is_dir:
            if self.size != other.size and \
                    self.size is not None and \
                    other.size is not None:
                return False
            if self.block_hashes != other.block_hashes and \
                    self.block_hashes is not None and \
                    other.block_hashes is not None:
                return False
        return True
