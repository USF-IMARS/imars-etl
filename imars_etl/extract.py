import os
import logging

from imars_etl.object_storage.imars_objects import imars_objects
from imars_etl.get_hook import get_metadata_hook
from imars_etl.util.config_logger import config_logger
from imars_etl.exceptions.TooManyMetadataMatchesException \
    import TooManyMetadataMatchesException

EXTRACT_DEFAULTS = {
}


class EXTRACT_METHOD:
    # any of the aliases in the list will work, but the first one is
    # the only "officially supported" usage.
    LINK = ["link", "ln"]
    COPY = ["copy", "cp"]


def extract(
    sql,
    output_path=None,
    first=False,
    verbose=0,
    method=EXTRACT_METHOD.COPY[0],
    **kwargs
):
    """
    Example usage:
    ./imars-etl.py -vvv extract 'area_id=1'
    """
    config_logger(verbose)
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
    ))

    full_sql_str = "SELECT filepath FROM file WHERE {}".format(sql)

    meta_hook = get_metadata_hook()
    try:
        result = meta_hook.get_records(
            full_sql_str,
            first=first,
        )
    except TooManyMetadataMatchesException:
        # === handle acceptable duplicate multihash entries (issue #41)
        if first:  # shouldn't get here if first == True anyway
            raise
        multihash_sql = full_sql_str.replace(
            "SELECT filepath FROM file ",
            "SELECT multihash FROM file "
        )
        multihashes = meta_hook.get_records(
            multihash_sql,
            check_result=False
        )
        # if all list elements are equal
        if multihashes.count(multihashes[0]) == len(multihashes):
            # we must re-query here bc first result was stoppered by exception
            result = meta_hook.get_records(
                full_sql_str,
                first=True,
            )

    src_path = result[0][0]

    if output_path is None:
        output_path = "./" + os.path.basename(src_path)

    logger.debug("extracting w/ method '{}'".format(method))

    if method.lower() in EXTRACT_METHOD.LINK:
        # TODO: could implement symlinking-fs as a ObjectStorage Hook
        #    & overload object_store instead
        # TODO: should assert src_path exists? or at least that it is path-like
        os.symlink(src_path, output_path)  # ln -s src_path output_path
        fpath = output_path
    elif method.lower() in EXTRACT_METHOD.COPY:
        object_storage = imars_objects()
        # use connection to download & then print a path to where the file can
        # be accessed on the local machine.
        fpath = object_storage.extract(
            src_path,
            target_path=output_path,
            first=False,
            **kwargs
        )
    else:
        raise ValueError(
            "Unknown extraction method \"{}\"".format(
                method.tolower()
            )
        )

    return fpath
