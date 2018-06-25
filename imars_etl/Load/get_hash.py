import subprocess


def get_hash(filepath):
    return _gethash_ipfs(filepath)


def _gethash_ipfs(filepath):
    """
    Get hash using IPFS system call.
    IPFS must be installed for this to work.
    """
    return subprocess.check_output([
        'ipfs', 'add',
        '-Q',  # --quieter    bool - Write only final hash
        '-n',  # --only-hash  bool - Only chunk and hash; do not write to disk
        filepath
    ]).strip().decode('utf-8')
