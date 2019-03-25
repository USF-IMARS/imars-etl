import datetime

from imars_etl.Load import constrain_dict
from imars_etl.util.timestrings import ISO_8601_FMT
from imars_etl.util.timestrings import standardize_time_str
from imars_etl.util.timestrings import iso8601strptime
from imars_etl.id_lookup import id_lookup


BASIC_METADATA_RELATION_CONSTRAINTS = [
    ('date_time', ['time'], iso8601strptime),
    ('time', ['date_time'], lambda dt: dt.strftime(ISO_8601_FMT)),
    (
        'product_type_name', ['product_id'],
        lambda p_id: id_lookup(p_id, 'product')
    ),
    ('product_short_name', ['product_type_name'], lambda ptn: ptn),
    (
        'product_id', ['product_type_name'],
        lambda p_name: id_lookup(p_name, 'product')
    ),
    ('area_id', ['area_short_name'], lambda a_id: id_lookup(a_id, 'area')),
    ('area_short_name', ['area_id'], lambda a_name: id_lookup(a_name, 'area')),
]

METADATA_TYPE_CASTS = [
    # | metadata_key | type test | list of (convert-able test , converter)
    # convert date_time string to datetime:
    (
        'date_time',
        lambda o: isinstance(o, datetime.datetime),
        [
            (lambda o: isinstance(o, str), lambda o: iso8601strptime(o))
        ]
    )
]


def ensure_metadata_types(metadat):
    """
    Ensures type restrictions are met in given metadata.
    Tries to convert types where possible.
    """
    updated_meta = metadat.copy()
    for cast in METADATA_TYPE_CASTS:
        meta_key = cast[0]
        updated_meta[meta_key] = _ensure_metadata_type(metadat, *cast)
    return updated_meta


def _ensure_metadata_type(metadat, meta_key, type_test, type_casts):
    meta_val = metadat.get(meta_key)
    if meta_val is not None:
        if type_test(meta_val) is False:
            # meta_val is bad type, try each converter:
            for typecast in type_casts:
                if typecast[0](meta_val):
                    return typecast[1](meta_val)
            else:
                # cannot convert
                raise TypeError((
                    "Value '{}' for metadata key '{}'" +
                    " is an unexpected datatype"
                ).format(
                    meta_val, meta_key
                ))
        else:
            return meta_val


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
