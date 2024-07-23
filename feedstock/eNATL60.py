import os
import logging
import xarray as xr
import apache_beam as beam
from leap_data_management_utils.data_management_transforms import (
    get_catalog_store_urls,
)

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
)

logger = logging.getLogger(__name__)

# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")

# if not run in a github workflow, assume local testing and deactivate the copy stage by setting all urls to False (see https://github.com/leap-stc/leap-data-management-utils/blob/b5762a17cbfc9b5036e1cd78d62c4e6a50c9691a/leap_data_management_utils/data_management_transforms.py#L121-L145)
if os.getenv("GITHUB_ACTIONS") == "true":
    print("Running inside GitHub Actions.")
else:
    print("Running locally. Deactivating final copy stage.")
    catalog_store_urls = {k: False for k in catalog_store_urls.keys()}

print("Final output locations")
print(f"{catalog_store_urls=}")


# Common Parameters
# days = range(1, 32)
# reduce for faster testing
days = range(1, 3)
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
        logger.info(f"Before preprocessing {ds=}")
        t_new = xr.DataArray(ds.time_counter.data, dims=["time"])
        ds = ds.assign_coords(time=t_new)
        ds = ds.drop(["time_counter"])
        ds = ds.set_coords(["deptht", "depthw", "nav_lon", "nav_lat", "tmask"])
        logger.info(f"After preprocessing {ds=}")
        return ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | "Fixes time coord" >> beam.MapTuple(
            lambda k, v: (k, self._set_coords(v))
        )


eNATL60_BLBT02 = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(max_concurrency=1)
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
    # | ConsolidateDimensionCoordinates()
    # | ConsolidateMetadata()
    # | Copy(target=catalog_store_urls["enatl60-blbt02"])
)
