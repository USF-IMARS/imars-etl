import pymysql


def get_conn():
    # get connection to the metadata database
    return pymysql.connect(
        host='192.168.1.41',
        user='imars_bot',
        password='***REMOVED***',  # noqa E501
        db='imars_product_metadata',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )
