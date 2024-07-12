import xarray as xr
import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    ConsolidateMetadata,
    ConsolidateDimensionCoordinates,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)


# Common Parameters
days = range(1, 32)
dataset_url = "https://zenodo.org/records/10513552/files"

## Monthly version
input_urls = [
    f"{dataset_url}/eNATL60-BLBT02_y2009m07d{d:02d}.1d_TSWm_60m.nc" for d in days
]
pattern = pattern_from_file_sequence(input_urls, concat_dim="time")


class Preprocess(beam.PTransform):
    """Custom transform to fix invalid time dimension"""

    @staticmethod
    def _set_coords(ds: xr.Dataset) -> xr.Dataset:
        t_new = xr.DataArray(ds.time_counter.data, dims=["time"])
        ds = ds.assign_coords(time=t_new)
        ds = ds.drop(["time_counter"])
        ds = ds.set_coords(["deptht", "depthw", "nav_lon", "nav_lat", "tmask"])

        return ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | "Fixes time coord" >> beam.MapTuple(
            lambda k, v: (k, self._set_coords(v))
        )


eNATL60_BLBT02 = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(
        xarray_open_kwargs={"use_cftime": True, "engine": "netcdf4"},
        load=True,
        copy_to_local=True,
    )
    | Preprocess()
    | StoreToZarr(
        store_name="eNATL60_BLBT02.zarr",
        combine_dims=pattern.combine_dim_keys,
        target_chunks={"x": 2000, "y": 2000, "time": 2},
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
)
