import hashlib
import logging

hash_algo = lambda block: hashlib.sha3_256(block).hexdigest()
block_size = 4 * 1024 * 1024

server_addr = 'http://mirrors.asia:8000/'

log_level = logging.INFO
log_format = '%(asctime)s.%(msecs)03d %(levelname)s %(message)s'

early_check_hash = True