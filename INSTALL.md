# Installation steps (short version):
1. non-pypi dependencies
    1. IPFS
        1. download IPFS from https://dist.ipfs.io/#go-ipfs
        2. unzip IPFS `tar xvfz go-ipfs.tar.gz`
        3. `cd go-ipfs`
        4. `./install.sh`
2. install imars-etl itself & pypi dependencies
    1. `git clone https://github.com/USF-IMARS/imars-etl`
    2. `cd imars-etl`
    3. `pip install -e .`

# dependency details
* IPFS is required for hashing files during load & may be used as an object
    storage driver in the future.
