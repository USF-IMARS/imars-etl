import pymysql

def get_conn():
    # get connection to the metadata database
    return pymysql.connect(
        host='reef02master',
        user='imars_bot',
        password='***REMOVED***',
        db='imars_product_metadata',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
