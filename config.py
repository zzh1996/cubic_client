import hashlib
import logging

hash_algo = lambda block: hashlib.sha3_256(block).hexdigest()
block_size = 4 * 1024 * 1024
upload_max_size = block_size * 20

log_level = logging.INFO
log_format = '%(asctime)s.%(msecs)03d %(levelname)s %(message)s'
