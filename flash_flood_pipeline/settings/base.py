from pathlib import Path

# references
DATA_FOLDER = Path("data/input_data")
ENVIRONMENT = "test"  # can be prod or test

# general
ASSET_TYPES = [
    "vulnerable_roads",
    "vulnerable_schools",
    "vulnerable_waterpoints",
    "vulnerable_buildings",
    "vulnerable_health_sites",
    "region_statistics",
]
KARONGA_PLACECODES = ["MW10106", "MW10104", "MW10203", "MW10220"]
RUMPHI_PLACECODES = [
    "MW10420",
    "MW10403",
    "MW10404",
    "MW10407",
    "MW10401",
    "MW10402",
    "MW10511",
    "MW10411",
]

BLANTYRE_PLACECODES = [
    "31546",
    "31545",
    "31541",
    "31548",
    "31552",
    "31540",
    "31549",
    "31543",
    "31533",
    "31539",
    "31531",
    "31553",
    "31544",
    "31542",
    "31551",
    "31537",
    "31536",
    "31535",
    "31534",
    "31538",
    "31547",
    "31550",
    "31532",
]

SMALL_LAGTIME_PLACECODES = [
    "MW10420",
    "MW10407",
    "31546",
    "31545",
    "31541",
    "31548",
    "31552",
    "31540",
    "31549",
    "31543",
    "31533",
    "31539",
    "31531",
    "31553",
    "31544",
    "31542",
    "31551",
    "31537",
    "31536",
    "31535",
    "31534",
    "31538",
    "31547",
    "31550",
    "31532",
]

EVENT_SEVERITY_ORDER = [
    (5, 1),
    (10, 12),
    (10, 4),
    (10, 2),
    (20, 12),
    (20, 4),
    (10, 1),
    (20, 2),
    (15, 1),
    (30, 12),
    (50, 48),
    (50, 24),
    (30, 4),
    (30, 2),
    (20, 1),
    (25, 1),
    (40, 12),
    (40, 4),
    (40, 2),
    (30, 1),
    (75, 24),
    (100, 48),
    (50, 12),
    (60, 12),
    (50, 4),
    (50, 2),
    (35, 1),
    (40, 1),
    (100, 24),
    (70, 12),
    (80, 12),
    (60, 4),
    (60, 2),
    (45, 1),
    (50, 1),
    (90, 12),
    (70, 4),
    (70, 2),
    (125, 24),
    (150, 48),
    (100, 12),
    (150, 24),
    (200, 48),
    (200, 24),
]

EVENT_SEVERITY_ORDER_STR = [
    "5mm_1hr",
    "10mm_12hr",
    "10mm_4hr",
    "10mm_2hr",
    "20mm_12hr",
    "20mm_4hr",
    "10mm_1hr",
    "20mm_2hr",
    "15mm_1hr",
    "30mm_12hr",
    "50mm_48hr",
    "50mm_24hr",
    "30mm_4hr",
    "30mm_2hr",
    "20mm_1hr",
    "25mm_1hr",
    "40mm_12hr",
    "40mm_4hr",
    "40mm_2hr",
    "30mm_1hr",
    "75mm_24hr",
    "100mm_48hr",
    "50mm_12hr",
    "60mm_12hr",
    "50mm_4hr",
    "50mm_2hr",
    "35mm_1hr",
    "40mm_1hr",
    "100mm_24hr",
    "70mm_12hr",
    "80mm_12hr",
    "60mm_4hr",
    "60mm_2hr",
    "45mm_1hr",
    "50mm_1hr",
    "90mm_12hr",
    "70mm_4hr",
    "70mm_2hr",
    "125mm_24hr",
    "150mm_48hr",
    "100mm_12hr",
    "150mm_24hr",
    "200mm_48hr",
    "200mm_24hr",
]

# alerts
ALERT_THRESHOLD_VALUE = 20
ALERT_THRESHOLD_PARAMETER = "affected_people"

# upload results
COUNTRY_CODE_ISO3 = "MWI"
DISASTER_TYPE = "flash-floods"

# CBFEWS forecast info
HISTORIC_TIME_PERIOD_DAYS = 4

CBFEWS_FORECAST_URL = "https://geoglows.ecmwf.int/api/ForecastStats/"
CBFEWS_MEASUREMENTS_URL = "http://malawi.cbfews.com/getStationData/"
CBFEWS_FORECASTS = [[7084744, "Karonga"], [7085570, "Mlowe"]]
CBFEWS_MEASUREMENTS = [
    ["MAL005", "Karonga"],
    ["MAL002", "Mlowe"],
]  # KARONGA SHOULD BE: MAL008

# Seba
SEBA_GAUGES = [98479]
SEBA_URL = "https://www.seba-hydrocenter.de/projects/listing.php?id={}"

# meteo
GFS_URL = "https://api.open-meteo.com/v1/gfs"
# METEO_SENSOR_LIST = [125720411]
WATERLEVEL_SENSOR = [125720411]
METEO_RAIN_SENSOR = "124107670"
