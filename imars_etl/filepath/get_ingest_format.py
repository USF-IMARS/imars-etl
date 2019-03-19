import json

from imars_etl.filepath.formatter_hardcoded.get_ingest_format import \
    get_ingest_format as hardcoded_get_ingest_format


def get_ingest_formats(metadb_handle, include_test_formats=True):
    """
    Returns a dict of all ingest formats.

    example:
    {
        "zip_wv2_ftp_ingest.matts_wv2_ftp_ingest": "wv2_%Y_%m_{tag}.zip",
        "att_.from_zip_ingest": "%y%b%d%H%M%S-M1BS-{idNum}_P{passNumber}.ATT",
    }

    deprecated alternative implementation at:
    from imars_etl.filepath.formatter_hardcoded.get_ingest_format import \
        get_ingest_formats
    """
    res = metadb_handle.get_records(
        sql="""
        SELECT product.short_name,path_format.short_name,params,format_string
        FROM product_formats
            INNER JOIN path_format
                ON path_format.id = product_formats.path_format_id
            INNER JOIN product
                ON product.id = product_formats.product_id
        ORDER BY priority DESC;
        """,
        first=False, check_result=False
    )
    res_dict = {}
    for (prod_name, path_name, params, fmt_str) in res:
        param_dict = json.loads(params)  # parse prefill params
        # double up on braces
        fmt_str = fmt_str.replace("{", "{{").replace("}", "}}")
        # remove double braces for prefilled args
        for prefill_param_key, prefill_param_val in param_dict.items():
            fmt_str = fmt_str.replace(
                "{" + prefill_param_key + "}", prefill_param_key
            )
        res_dict["{}.{}".format(prod_name, path_name)] = fmt_str.format(
            **param_dict
        )
    print(res_dict.keys())
    if include_test_formats:
        res_dict.update({
            'test_test_test.file_w_date': 'file_w_date_%Y.txt',
            # 'test_test_test.imars_object_format':
            #    'test_test_test/simple_file_with_no_args.txt',
            'test_fancy_format_test.file_w_date':
                'date_%Y%j.arg_{test_arg}.time_%H%S.woah',
            # 'test_fancy_format_test.imars_object_format':
            #    '_fancy_{test_arg}_/%Y-%j/arg_is_{test_arg}_time_is_%H%S.fancy_file',
            'test_number_format_test.file_w_formatted_nums':
                'date_%Y.num_{test_num:3d}.time_%H.dude',
            # 'test_number_format_test.imars_object_format':
            #    '_fancy_{test_num:0>3d}_/%Y/num_is_{test_num2:0>4d}_time_is_%H.fancy_file',
        })
    return res_dict


def get_ingest_format(short_name, ingest_name=None):
    """
    Returns
    -------
    ingest_fmt_str : str
        ingest path format string for given product short_name and ingest_name.
    ingest_name : str
        name of the ingest_key used. Only useful if ingest_name=None,
        else it just passes through unaltered.
    """
    return hardcoded_get_ingest_format(short_name, ingest_name)
