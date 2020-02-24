# Requirements:
1. python 3.2+
    * for `functools.lru_cache`. It may be possible to use older py by installing `functools32`.

# Installation steps (short version):
1. non-pypi dependencies
    1. IPFS
        1. download IPFS from https://dist.ipfs.io/#go-ipfs
        2. unzip IPFS `tar xvfz go-ipfs.tar.gz`
        3. `cd go-ipfs`
        4. `./install.sh`
        5. `ipfs init`
    2. `gcc`
        * -buntu: `sudo apt install gcc`
        * centos: `sudo yum install gcc`
        * arch  : `sudo pacman install gcc`
    3. (skippable) manual set up for airflow (no longer required as of v1.10.4)
        1. `export SLUGIFY_USES_TEXT_UNIDECODE=yes`
2. install imars-etl itself & pypi dependencies
    1. `git clone https://github.com/USF-IMARS/imars-etl`
    2. `cd imars-etl`
    3. `pip install -e .[test]`
        * NOTE: if using `sudo` you need to include `-E`
3. init airflow database (for connections):
    * `airflow initdb`
4. set up connections. At a minimum set a default for metadata & object stores (`imars_metadata` and `imars_object_store`).
    0. install any [airflow subpackages](https://airflow.readthedocs.io/en/stable/installation.html) required by your connections eg:
        * `pip install apache-airflow[mysql]`
    1. Example metadata connection: `/bin/airflow connections -a --conn_id imars_metadata --conn_uri mysql://mysql_user:FANCYpassword123@mysql-server/imars_product_metadata`
    2. Example object storage connection: `/bin/airflow connections -a --conn_id imars_object_store --conn_uri azure_data_lake://azure_user:AZUREpw123@azure_srv/lakename`

# dependency details
* IPFS is required for hashing files during load & may be used as an object
    storage connection in the future.
* airflow is used to manage secure connections to metadata & object stores.

# testing the installation
```
# run all tests that don't require a real metadata db
python3 -m pytest -m "not metadatatest"

# run all tests
python3 -m pytest
```
