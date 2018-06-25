class TooManyMetadataMatchesException(LookupError):
    """
    More entries with metadata matching what was requested found than were
    expected.

    Often we expect only 1 match, and if more than one is found this exception
    should be raised.
    """
