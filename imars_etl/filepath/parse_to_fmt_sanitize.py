def parse_to_fmt_sanitize(format_str):
    """
    # rm format specs supported by parse() but not format()
    #   avoids "ValueError: Unknown format code ... for object of type..."
    """
    return format_str.replace(":w}", "}")
