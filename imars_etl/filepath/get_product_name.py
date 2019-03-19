from imars_etl.filepath.formatter_hardcoded.get_product_name import \
    get_product_name as hardcoded_get_product_name


def get_product_name(product_id):
    """Get product name from given id"""
    return hardcoded_get_product_name(product_id)
