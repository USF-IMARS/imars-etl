from imars_etl.filepath.data import data

def get_product_id(product_type_name):
    """get product id from given name"""
    return data[product_type_name]["imars_object_format"]["product_id"]
