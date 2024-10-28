EXPOSURE_TYPES = {
    "affected_people": "population_affected",
    "high risk_buildings": "nr_affected_buildings",
    "high risk_roads": "nr_affected_roads",
    "high risk_schools": "nr_affected_schools",
    "high risk_hospitals": "nr_affected_clinics",
    "high risk_waterpoints": "nr_affected_waterpoints",
    "total_damage": "damage_estimation",
}

TA_EXPOSURE_DICT = {
    "countryCodeISO3": "MWI",
    "exposurePlaceCodes": [],
    "adminLevel": 3,
    "leadTime": "3-hour",
    "dynamicIndicator": "",
    "disasterType": "flash-floods",
    "eventName": None,
}

POINT_EXPOSURE_DICT = {
    "exposedFids": [],
    "leadTime": "3-hour",
    "countryCodeISO3": "MWI",
    "disasterType": "flash-floods",
    "pointDataCategory": "",
}

DYNAMIC_POINT_EXPOSURE_DICT = {
    "leadTime": "3-hour",
    "disasterType": "flash-floods",
    "pointDataCategory": "",
}


GEOSERVER_EXPOSURE_DICT = {
    "exposedFids": [],
    "leadTime": "3-hour",
    "countryCodeISO3": "MWI",
    "disasterType": "flash-floods",
    "linesDataCategory": "",
}
