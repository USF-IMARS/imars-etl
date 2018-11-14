import logging
import sys

from imars_etl.exceptions.NoMetadataMatchException \
    import NoMetadataMatchException
from imars_etl.exceptions.TooManyMetadataMatchesException \
    import TooManyMetadataMatchesException
from imars_etl.exceptions.DuplicateFileException \
    import DuplicateFileException

from imars_etl.select import select
from imars_etl.Load.get_hash import get_hash

HASH_COL_NAME = 'multihash'


def hashcheck(filepath=None, multihash=None, **kwargs):
    """
    Returns False iff file at filepath is not already in metadatadb.

    Else raises DuplicateFileException
    """
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )
    logger.info('computing hash of file...')
    # get hash of file we are loading
    if multihash is not None:
        file_hash = multihash
    elif filepath is not None:
        file_hash = get_hash(filepath)
    else:
        assert(filepath is None and multihash is None)
        raise ValueError(
            "One of 'filepath' or 'multihash' must be passed to hashcheck"
        )
    # check for hash in metadatadb
    try:
        select(
            "{}='{}'".format(
                HASH_COL_NAME, file_hash
            )
        )
    except NoMetadataMatchException:
        return False
    except TooManyMetadataMatchesException as too_ex:
        raise DuplicateFileException(
            "multiple files found in metadata db with same file content",
            too_ex
        )
    raise DuplicateFileException(
        "file found in metadata db with identical content"
    )
