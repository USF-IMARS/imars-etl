try:  # py3
    ParentException = FileExistsError
except NameError:  # py2
    ParentException = OSError


class DuplicateFileException(ParentException):
    """
    Raised when user trying to add metadata entry for a file that is already
    in the metdata db.

    This could be from checking file paths or file hashes.
    """
