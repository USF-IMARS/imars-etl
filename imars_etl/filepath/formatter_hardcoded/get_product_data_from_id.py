from imars_etl.filepath.formatter_hardcoded.data import data
from imars_etl.filepath.get_product_name import get_product_name


def get_product_data_from_id(prod_id):
    """
    !!! DEPRECATED !!!
     Used to index product data by id number instead of short_name
     """
    return data[get_product_name(prod_id)]
