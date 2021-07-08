def is_byte_decode(input, encoding="utf-8"):
    if isinstance(input, bytes):
        return input.decode(encoding)
    return input