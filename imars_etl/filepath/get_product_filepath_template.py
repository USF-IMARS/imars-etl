from imars_etl.filepath.formatter_hardcoded.get_product_filepath_template \
    import get_product_filepath_template \
    as hardcoded_get_product_filepath_template


def get_product_filepath_template(
    product_type_name=None,
    product_id=None,
    forced_basename=None
):
    return hardcoded_get_product_filepath_template(
        product_type_name, product_id, forced_basename
    )
