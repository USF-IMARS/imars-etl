from imars_etl.Load import constrain_dict
from imars_etl.util.timestrings import ISO_8601_FMT
from imars_etl.util.timestrings import standardize_time_str
from imars_etl.util.timestrings import iso8601strptime
from imars_etl.filepath.get_product_id import get_product_id
from imars_etl.filepath.get_product_name import get_product_name


BASIC_METADATA_RELATION_CONSTRAINTS = [
    ('date_time', ['time'], iso8601strptime),
    ('time', ['date_time'], lambda dt: dt.strftime(ISO_8601_FMT)),
    ('product_type_name', ['product_id'], get_product_name),
    ('product_id', ['product_type_name'], get_product_id),
]


def ensure_constistent_metadata(
    metad,
    raise_cannot_constrain=False,
):
    # precheck:
    if metad.get('time') is not None:
        metad['time'] = standardize_time_str(metad['time'])

    # check
    metad = _ensure_constistent_metadata(
        metad,
        BASIC_METADATA_RELATION_CONSTRAINTS,
        raise_cannot_constrain=raise_cannot_constrain,
    )

    # postcheck NOTE: same as precheck
    if metad.get('time') is not None:
        metad['time'] = standardize_time_str(metad['time'])

    return metad


def _ensure_constistent_metadata(
    metad,
    relations,
    raise_cannot_constrain=False,
):
    """
    Ensures metadata values are consistent with other metadata values.
    If a value is missing but can be inferred from other values, it is filled.
    Raises exception if two values are found to be inconsistent.
    """
    for constraint in relations:
        metad = constrain_dict.relation(
            metad, *constraint, raise_cannot_constrain=raise_cannot_constrain
        )
    return metad
