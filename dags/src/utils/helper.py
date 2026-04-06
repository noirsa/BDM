def get_deep_keys(data,level=0):
    # If it's a list, dive into the first element
    if isinstance(data, list) and len(data) > 0:
        return get_deep_keys(data[0],level+1)

    # If it's finally a dictionary, return the keys
    if isinstance(data, dict):
        return list(data.keys()),level

    # If it's a primitive (like a string or number) or empty
    return [],level

