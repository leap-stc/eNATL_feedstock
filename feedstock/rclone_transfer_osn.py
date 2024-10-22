# slow boat method for transfer from THREDDS to OSN
# We could parallelize this method with beam / dask

import pandas as pd
import subprocess
from tqdm import tqdm

dates = pd.date_range("2009-07-01", "2010-06-30", freq="D")


def make_full_path(time):
    date_fmt = time.strftime("y%Ym%md%d")
    return f"https://ige-meom-opendap.univ-grenoble-alpes.fr/thredds/fileServer/meomopendap/extract/MEOM/eNATL60/eNATL60-BLBT02/1d//eNATL60-BLBT02_{date_fmt}.1d_TSWm_600m.nc"


input_list = [make_full_path(time) for time in dates]

status_code = []
url_code = []
for file_url in tqdm(input_list):
    command = [
        "rclone",
        "copyurl",
        file_url,
        "osnmanual:leap-pangeo-manual/eNATL_600m",
        "--checksum",
        "--auto-filename",
    ]
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    status_code.append(result.returncode)
    url_code.append(file_url)
