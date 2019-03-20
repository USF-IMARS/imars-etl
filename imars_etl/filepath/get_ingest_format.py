import json


def _prefill_fmt_str(fmt_str, params):
    param_dict = json.loads(params)  # parse prefill params
    # double up on braces
    fmt_str = fmt_str.replace("{", "{{").replace("}", "}}")
    # remove double braces for prefilled args
    for prefill_param_key, prefill_param_val in param_dict.items():
        fmt_str = fmt_str.replace(
            "{" + prefill_param_key + "}", prefill_param_key
        )
    return fmt_str.format(
        **param_dict
    )


def get_ingest_format(**kwargs):
    return get_ingest_formats(
        **kwargs, check_result=True
    )


def get_ingest_formats(
    metadb_handle,
    short_name=None, ingest_name=None, product_id=None,
    include_test_formats=True,
    first=False, check_result=False
):
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
    if short_name is None and ingest_name is None and product_id is None:
        where_clause = ""
    else:
        where_clause = " WHERE "
        n_clauses = 0
        if short_name is not None:
            where_clause += " product.short_name='{}' ".format(short_name)
            n_clauses += 1
        if ingest_name is not None:
            if n_clauses > 0:
                andy = "AND"
            else:
                andy = ""
            where_clause += " {} path_format.short_name='{}' ".format(
                andy,
                ingest_name
            )
            n_clauses += 1
        if product_id is not None:
            if n_clauses > 0:
                andy = "AND"
            else:
                andy = ""
            where_clause += " {} product.product_id={} ".format(
                andy,
                int(product_id)
            )
            n_clauses += 1
    res = metadb_handle.get_records(
        sql="""
        SELECT product.short_name,path_format.short_name,params,format_string
        FROM product_formats
            INNER JOIN path_format
                ON path_format.id = product_formats.path_format_id
            INNER JOIN product
                ON product.id = product_formats.product_id
        {}
        ORDER BY priority DESC;
        """.format(where_clause),
        first=first, check_result=check_result
    )
    res_dict = {}
    for (prod_name, path_name, params, fmt_str) in res:
        # NOTE: path_name AKA ingest_key
        res_dict["{}.{}".format(prod_name, path_name)] = _prefill_fmt_str(
            fmt_str,
            params
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
