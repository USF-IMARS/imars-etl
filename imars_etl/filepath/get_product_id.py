from imars_etl.filepath.formatter_hardcoded.get_product_id import \
    get_product_id as hardcoded_get_product_id


def get_product_id(product_type_name):
    """Get product id from given name"""
    return hardcoded_get_product_id(product_type_name)
