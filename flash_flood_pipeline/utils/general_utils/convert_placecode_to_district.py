from settings.base import (
    KARONGA_PLACECODES,
    RUMPHI_PLACECODES,
    BLANTYRE_PLACECODES,
)


def convert_placecode_to_district(place_code):
    if place_code in KARONGA_PLACECODES:
        return "Karonga"
    elif place_code in RUMPHI_PLACECODES:
        return "Rumphi"
    elif place_code in BLANTYRE_PLACECODES:
        return "Blantyre City"
    else:
        return None
