POST_MARKER_FMT = "._fmt({})"


def parse_to_fmt_sanitize(format_str, preserve_parse_fmts=False):
    """
     rm format specs supported by parse() but not format()
       avoids "ValueError: Unknown format code ... for object of type..."

    parameters:
    -----------
    preserve_parse_fmts: bool
        Pass True to inject variable suffixes that preserve the sanitized
        parse specificiation. Useful for partial string formatting.
        Use restore_parse_fmts() to replace the spec strings.

    TODO: only :w is sanitized here. ought to add more if they will be used.
    """
    if preserve_parse_fmts is False:
        format_str = format_str.replace(":w}", "}")
    else:
        format_str = format_str.replace(
            ":w}", "}" + POST_MARKER_FMT.format(":w")
        )
    return format_str


def restore_parse_fmts(format_str):
    """
    Puts format specs sanitized by parse_to_fmt_sanitize() with
    preserve_parse_fmts=True back into the proper format.

    Useful for partial string formatting
        https://stackoverflow.com/questions/11283961/partial-string-formatting

    Example:
    --------
        ```python
        sanitized = parse_to_fmt_sanitize(
            "{word:w} and {var:w}", preserve_parse_fmts=True
        )
        # sanitized == "{word}._fmt(:w) and {var}._fmt(:w)"
        formatted = sanitized.format(word="{word}", var="the variable")
        # formatted == "{word}._fmt(:w) and the variable._fmt(:w)"
        restored = restore_parse_fmts(formatted)
        # restored == "{word:w} and the variable"
        ```
    """
    # inject format specs back in if {vars} still in str
    format_str = format_str.replace("}" + POST_MARKER_FMT.format(":w"), ":w}")
    # drop out format specs which have been ignored
    format_str = format_str.replace(POST_MARKER_FMT.format(":w"), "")
    return format_str
