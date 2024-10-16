import xarray as xr
import pandas as pd
import apache_beam as beam
import pooch
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import (
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
    OpenWithXarray,
    OpenURLWithFSSpec,
    StoreToZarr,
)

from leap_data_management_utils.data_management_transforms import (
    get_catalog_store_urls,
    Copy,
)

catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")


dates = pd.date_range("2009-07-01", "2010-06-30", freq="D")


def make_full_path(time):
    date_fmt = time.strftime("y%Ym%md%d")
    return f"https://ige-meom-opendap.univ-grenoble-alpes.fr/thredds/fileServer/meomopendap/extract/MEOM/eNATL60/eNATL60-BLBT02/1d/eNATL60/eNATL60-BLBT02_{date_fmt}.1d_TSWm_600m.nc"


time_concat_dim = ConcatDim("time", dates)
pattern = FilePattern(make_full_path, time_concat_dim)


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
        ds = ds.rename({"time_counter": "time"})
        ds = ds.set_coords(("nav_lat", "nav_lon"))
        ds.attrs["deptht"] = ds.deptht.values[0]
        ds = ds.drop("deptht")
        ds = ds[["vosaline", "votemper", "vovecrtz"]]
        return ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | "Fixes time coord" >> beam.MapTuple(
            lambda k, v: (k, self._set_coords(v))
        )


eNATL600BLBT02 = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(load=True, copy_to_local=True)
    | Preprocess()
    | StoreToZarr(
        store_name="eNATL600m-BLBT02.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"time": 100, "y": 400, "x": 800},
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | Copy(target=catalog_store_urls["enatl600m-blbt02"])
)
