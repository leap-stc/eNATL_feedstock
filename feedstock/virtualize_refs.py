# pip install fastparquet kerchunk dask distributed pandas xarray git+https://github.com/zarr-developers/VirtualiZarr netcdf4

import xarray as xr
from virtualizarr import open_virtual_dataset
import dask
from dask.distributed import Client
import pandas as pd


client = Client(n_workers=16)
client


# generate input list from osn files:
dates = pd.date_range("2009-07-01", "2010-06-30", freq="D")


def make_full_path(time):
    date_fmt = time.strftime("y%Ym%md%d")
    return f"https://nyu1.osn.mghpcc.org/leap-pangeo-manual/eNATL_600m/eNATL60-BLBT02_{date_fmt}.1d_TSWm_600m.nc"


input_list = [make_full_path(time) for time in dates]


def process(filename):
    vds = open_virtual_dataset(
        filename, loadable_variables=["x", "y", "time", "depth"], indexes={}
    )
    vds = vds.rename({"time_counter": "time"})
    vds = vds.set_coords(("nav_lat", "nav_lon"))
    vds = vds.drop_vars("deptht")
    vds = vds[["vosaline", "votemper", "vovecrtz"]]
    return vds


delayed_results = [dask.delayed(process)(filename) for filename in input_list]

# compute delayed obs
results = dask.compute(*delayed_results)

# concat virtual datasets
combined_vds = xr.concat(list(results), dim="time", coords="minimal", compat="override")

# once icechunk PR is in, we can write to Zarr v3
combined_vds.virtualize.to_kerchunk(
    "gs://leap-persistent/norlandrhagen/references/enatl_600m.parquet", format="parquet"
)

client.close()
