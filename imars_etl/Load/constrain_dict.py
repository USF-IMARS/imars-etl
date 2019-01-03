import logging


def relation(
    dadict, outkey, inkeys, getter,
    raise_cannot_constrain=False,
):
    """
    Adds a relationship constraint between keys to the given dict 'dadict'
    enforcing something like:
    `dadict['outkey'] == getter(dadict[inkeys[0]], dadict[inkeys[1]], ...)`

    If dadict['outkey'] is None or empty it is set by the getter.
    If dadict['outkey'] exists it is checked against expected value from
        getter.
    Raises if one or more of the 'inkeys' is not in dadict.
    """
    logger = logging.getLogger("imars_etl.{}".format(
        __name__,
        )
    )

    in_vals = [dadict.get(ink) for ink in inkeys]
    if not all([inv is not None for inv in in_vals]):
        if raise_cannot_constrain:
            raise AssertionError("all inputs not in da dict mon")
        else:
            logger.debug(
                "skipping constraint on {}. missing 1+ of keys {}".format(
                    outkey, inkeys
                )
            )
            return dadict

    if dadict.get(outkey) is None:
        # fill empty output
        val = getter(*in_vals)
        logger.debug("setting '{}' = {}".format(outkey, val))
        dadict[outkey] = val
    else:
        # check expected output matches
        expect = getter(*in_vals)
        actual = dadict[outkey]
        if actual != expect:
            raise ValueError(
                "expected output does not match actual:" +
                "\n\tactual: {}".format(actual) +
                "\n\texpect: {}".format(expect)
            )
        logger.debug("key {} validated by relation to {}".format(
            outkey, inkeys
        ))
    return dadict


def contains(da_dict, keys):
    """
    given dict must have each key in keys with non-None values.
    """
    assert all([da_dict.get(key) is not None for key in keys])
