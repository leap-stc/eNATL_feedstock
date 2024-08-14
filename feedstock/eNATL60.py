import xarray as xr
import pandas as pd
import apache_beam as beam
import pooch
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import (
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
    OpenWithXarray,
    StoreToZarr,
)

from leap_data_management_utils.data_management_transforms import (
    Copy,
    get_catalog_store_urls,
)

catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")
dates = pd.date_range("2009-07-01", "2010-06-30", freq="D")

records = {
    1: "10261988",
    2: "10260907",
    3: "10260980",
    4: "10261078",
    5: "10261126",
    6: "10261192",
    7: "10261274",
    8: "10261349",
    9: "10261461",
    10: "10261540",
    11: "10262356",
    12: "10261643",
}


def make_full_path(time):
    record = str(records[time.month])
    date = (
        "y"
        + str(time.year)
        + "m"
        + str("{:02d}".format(time.month))
        + "d"
        + str("{:02d}".format(time.day))
    )
    return (
        f"https://zenodo.org/records/{record}/files/eNATL60-BLBT02_{date}.1d_TSW_60m.nc"
    )


time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_full_path, time_concat_dim)
pattern = pattern.prune(120)


class OpenWithPooch(beam.PTransform):
    @staticmethod
    def _open_pooch(url: str) -> str:
        return pooch.retrieve(url=url, known_hash=None)

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | "open" >> beam.MapTuple(lambda k, v: (k, self._open_pooch(v)))


class Preprocess(beam.PTransform):
    """Custom transform to fix invalid time dimension"""

    @staticmethod
    def _set_coords(ds: xr.Dataset) -> xr.Dataset:
        t_new = xr.DataArray(ds.time_counter.data, dims=["time"])
        ds = ds.assign_coords(time=t_new)
        ds = ds.drop(["time_counter"])
        ds = ds.set_coords(["deptht", "depthw", "nav_lon", "nav_lat"])

        # # convert cftime.DatetimeGregorian to datetime64[ns]
        # ds["time"] = ds.indexes["time"].to_datetimeindex()

        return ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | "Fixes time coord" >> beam.MapTuple(
            lambda k, v: (k, self._set_coords(v))
        )


eNATL60BLBT02 = (
    beam.Create(pattern.items())
    # | OpenURLWithFSSpec(max_concurrency=1)
    | OpenWithPooch()
    | OpenWithXarray()
    # xarray_open_kwargs={"use_cftime": True, "engine": "netcdf4"},
    # load=True,
    # copy_to_local=True,)
    | Preprocess()
    | StoreToZarr(
        store_name="eNATL60-BLBT02.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"time": 30, "y": 900, "x": 900},
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target=catalog_store_urls["enatl60-blbt02"])
)
