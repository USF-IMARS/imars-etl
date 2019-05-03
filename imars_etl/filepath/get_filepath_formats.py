import json
import logging
from pprint import pformat
import re
from functools import lru_cache

from imars_etl.filepath.parse_to_fmt_sanitize import parse_to_fmt_sanitize
from imars_etl.filepath.parse_to_fmt_sanitize import restore_parse_fmts


def _prefill_fmt_str(fmt_str, params):
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
    ))
    param_dict = json.loads(params)  # parse prefill params
    logger.trace("fmt_str  : {}".format(fmt_str))
    fmt_str = parse_to_fmt_sanitize(fmt_str, preserve_parse_fmts=True)
    logger.trace("sanitized: {}".format(fmt_str))
    # double up on braces
    fmt_str = fmt_str.replace("{", "{{").replace("}", "}}")
    # remove double braces for prefilled args
    for prefill_param_key, prefill_param_val in param_dict.items():
        # make double brackets into single brackets for instances of
        #   {prefill_param_key}, {prefill_param_key:1d} or similar fmt strings
        fmt_str = re.sub(
            r"\{" +
            prefill_param_key +
            r"(\:*)([0-9,a-z,A-Z,>,<,=,^,+,-,_]{,3})\}",
            prefill_param_key + r"\1\2",
            fmt_str
        )
    prefilled_str = fmt_str.format(
        **param_dict
    )
    logger.trace("prefilled: {}".format(prefilled_str))
    restored_str = restore_parse_fmts(prefilled_str)
    logger.trace("restored : {}".format(restored_str))
    return restored_str


def _get_test_formats_dict():
    return {
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
    }


def _get_test_formats_records():
    # product.short_name, path_format.short_name, params, format_string
    records = []
    for key, val in _get_test_formats_dict().items():
        records.append([*key.split('.'), '{}', val])
    return records


def get_ingest_format(**kwargs):
    return get_filepath_formats(
        **kwargs, check_result=True
    )


@lru_cache(maxsize=None)
def get_filepath_formats(
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
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
    ))
    logger.trace('get_filepath_formats({},{},{})'.format(
        short_name, ingest_name, product_id
    ))

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
            where_clause += " {} product.id={} ".format(
                andy,
                int(product_id)
            )
            n_clauses += 1
    result = metadb_handle.get_records(
        sql="""
        SELECT product.short_name,path_format.short_name,params,format_string
        FROM product_formats
            INNER JOIN path_format
                ON path_format.id=product_formats.path_format_id
            INNER JOIN product
                ON product.id=product_formats.product_id
        {}
        ORDER BY priority DESC
        """.format(where_clause),
        first=first, check_result=check_result
    )
    # if first:
    #     result = [result]
    logger.debug(result)
    if len(result) > 0 and len(result[0]) != 4:
        raise AssertionError("misshapen results!?!")
    res_dict = {}
    for res in result:
        logger.trace("(prod_name, path_name, params, fmt_str)={}".format(res))
        (prod_name, path_name, params, fmt_str) = res
        # NOTE: path_name AKA ingest_key
        res_dict["{}.{}".format(prod_name, path_name)] = _prefill_fmt_str(
            fmt_str,
            params
        )

    # if Logger.isEnabledFor(logging.INFO):
    logger.debug("matching formats:\n{}".format(
        pformat(list(res_dict.keys())), width=1, indent=2)
    )
    if include_test_formats:
        res_dict.update(_get_test_formats_dict())
    return res_dict
