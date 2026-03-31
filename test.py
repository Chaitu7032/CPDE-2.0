import os
import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

load_dotenv()

url = "https://e4ftl01.cr.usgs.gov/MOLT/MOD11A1.061/2026.02.23/MOD11A1.A2026054.h25v07.061.2026055093025.hdf"

r = requests.get(
    url,
    auth=HTTPBasicAuth(
        os.getenv("EARTHDATA_USERNAME"),
        os.getenv("EARTHDATA_PASSWORD")
    ),
    stream=True
)

print("Status code:", r.status_code)