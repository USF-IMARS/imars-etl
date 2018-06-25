import base58
import hashlib


with open('README.md') as fhand:
    hsh = hashlib.sha256(fhand.read()).digest()
    print(hsh)
    b58 = base58.b8encode(hsh)
    print(b58)
