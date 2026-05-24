def get_deep_keys(data, level=0):
    """Return top-level keys from the first nested JSON object and its list depth."""
    if isinstance(data, list) and len(data) > 0:
        return get_deep_keys(data[0], level + 1)

    if isinstance(data, dict):
        return list(data.keys()), level

    return [], level
