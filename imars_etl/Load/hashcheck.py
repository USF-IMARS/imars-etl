import logging
import sys

from imars_etl.get_metadata import get_metadata
from imars_etl.exceptions.NoMetadataMatchException \
    import NoMetadataMatchException
from imars_etl.exceptions.TooManyMetadataMatchesException \
    import TooManyMetadataMatchesException
from imars_etl.exceptions.DuplicateFileException \
    import DuplicateFileException
from imars_etl.Load.get_hash import get_hash

HASH_COL_NAME = 'multihash'


def hashcheck(filepath, **kwargs):
    """
    Returns False iff file at filepath is not already in metadatadb.

    Else raises DuplicateFileException
    """
    logger = logging.getLogger("{}.{}".format(
        __name__,
        sys._getframe().f_code.co_name)
    )
    logger.info('computing hash of file...')
    # get hash of file we are loading
    file_hash = get_hash(filepath)
    # check for hash in metadatadb
    try:
        get_metadata(
            sql="{}='{}'".format(
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
