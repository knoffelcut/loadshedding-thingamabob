class ParseError(ValueError):
    pass

def extract_eskom_loadshedding_stage(response_text: str):
    """Extracts the current Eskom loadshedding stage

    Args:
        response_text (str): Eskom API response

    Returns:
        int: Current national loadshedding stage
    """
    try:
        stage = int(response_text)
        stage = stage - 1
        return stage
    except Exception as e:
        raise ParseError from e
