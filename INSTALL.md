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
    3. `pip install .[test]`
3. set up connections. At a minimum set a default for metadata & object stores (`imars_metadata` and `imars_object_store`).
    1. Example metadata connection: `/bin/airflow connections -a --conn_id imars_metadata --conn_uri mysql://mysql_user:FANCYpassword123@mysql-server/imars_product_metadata`
    2. Example object storage connection: `/bin/airflow connections -a --conn_id imars_object_store --conn_uri azure_data_lake://azure_user:AZUREpw123@azure_srv/lakename`

# dependency details
* IPFS is required for hashing files during load & may be used as an object
    storage connection in the future.
* airflow is used to manage secure connections to metadata & object stores.
