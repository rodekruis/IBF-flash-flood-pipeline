from enums.precipitation_sum import PrecipitationSum


def event_mapping_12hr(number):
    """
    Map the precipiation sum amount for an 12hr event to a value of 10,20,30,40,50,60,70,80,90 or max 100 mm.

    Args:
        number (float): precipitation sum amount to apply the mapping to

    Returns:
        Mapped number (int): Precipiation sum amount according to a defined 12hr event
    """
    rounded_number = round(number / 10) * 10
    return min(rounded_number, PrecipitationSum.UPPER_VALUE_12HR_EVENT.value)


def event_mapping_24hr(number):
    """
    Map the precipiation sum amount for an 24hr event to a value of 25,50, 75, 100, 125, 150, 175 or max 200 mm.

    Args:
        number (float): precipitation sum amount to apply the mapping to

    Returns:
        Mapped number (int): Precipiation sum amount according to a defined 24hr event
    """
    rounded_number = round(number / 25) * 25
    
    if min(rounded_number, PrecipitationSum.UPPER_VALUE_24HR_EVENT.value) == 25:
        number = 0
    else:
        number = min(rounded_number, PrecipitationSum.UPPER_VALUE_24HR_EVENT.value)
    return number


def event_mapping_48hr(number):
    """
    Map the precipiation sum amount for an 48hr event to a value of 50, 100, 150 or max 200 mm.

    Args:
        number (float): precipitation sum amount to apply the mapping to

    Returns:
        Mapped number (int): Precipiation sum amount according to a defined 48hr event
    """
    
    rounded_number = round(number / 50) * 50

    if number < 37.5: # CUSTOM TEMPORARY MAPPING 
        return 0
    else:
        return min(rounded_number, PrecipitationSum.UPPER_VALUE_48HR_EVENT.value)


def event_mapping_4hr(number):
    """
    Map the precipiation sum amount for an 4hr event to a value of 10,20,30,40,50,60,70 or max 80 mm.

    Args:
        number (float): precipitation sum amount to apply the mapping to

    Returns:
        Mapped number (int): Precipiation sum amount according to a defined 4hr event
    """
    rounded_number = round(number / 10) * 10
    return min(rounded_number, PrecipitationSum.UPPER_VALUE_4HR_EVENT.value)


def event_mapping_2hr(number):
    """
    Map the precipiation sum amount for an 2hr event to a value of 10,20,30,40,50,60 or max 70 mm.

    Args:
        number (float): precipitation sum amount to apply the mapping to

    Returns:
        Mapped number (int): Precipiation sum amount according to a defined 2hr event
    """
    rounded_number = round(number / 10) * 10
    return min(rounded_number, PrecipitationSum.UPPER_VALUE_2HR_EVENT.value)


def event_mapping_1hr(number):
    """
    Map the precipiation sum amount for an 1hr event to a value of 5,10,15,20,25,30,35,40,45 or max 50 mm.
    TEMPORAL CORRECTION: Map the precipiation sum amount for an 1hr event to a value of 10,20,30,40 or max 50 mm.

    Args:
        number (float): precipitation sum amount to apply the mapping to

    Returns:
        Mapped number (int): Precipiation sum amount according to a defined 1hr event
    """
    rounded_number = round(number / 10) * 10
    return min(rounded_number, PrecipitationSum.UPPER_VALUE_1HR_EVENT.value)
