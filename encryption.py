from Cryptodome.Cipher import AES
from Cryptodome.Hash import HMAC, SHA256, SHA3_256


class Encryption:
    def __init__(self, key):
        self.key = SHA3_256.new().update(key.encode()).digest()[:16]

    def hmac(self, data):
        return HMAC.new(self.key, digestmod=SHA256).update(data).digest()

    def encrypt(self, data):
        nonce = self.hmac(data)
        ciphertext, tag = AES.new(self.key, AES.MODE_GCM, nonce=nonce).encrypt_and_digest(data)
        return nonce + tag + ciphertext

    def decrypt(self, data):
        nonce = data[:32]
        tag = data[32:32 + 16]
        ciphertext = data[32 + 16:]
        return AES.new(self.key, AES.MODE_GCM, nonce=nonce).decrypt_and_verify(ciphertext, tag)
