def handle_to_array(value):
    assert isinstance(value, str)
    return value.strip().split(',')


custom_transformers = {
    'to_array': handle_to_array
}
