import apache_beam as beam
import xarray as xr
from dataclasses import dataclass

from pangeo_forge_ndpyramid.transforms import StoreToPyramid
from pangeo_forge_recipes.transforms import OpenWithXarray, ConsolidateMetadata
from pangeo_forge_recipes.patterns import FileType, pattern_from_file_sequence

from leap_data_management_utils.data_management_transforms import (
    Copy,
    get_catalog_store_urls,
)

# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")


# How many pyramid levels
# spatial resolution from ds.attrs seems to be 1/4 degree or at equator (~111000 meters * 1/4)
# import morecantile
# tms = morecantile.tms.get("WebMercatorQuad")
# lvls = tms.zoom_for_res(111000/4)
# > 2
levels = 2


pattern = pattern_from_file_sequence(
    [
        "gs://leap-persistent/data-library/feedstocks/eNATL_feedstock/eNATL60-BLBT02.zarr"
    ],
    concat_dim="time",
)


@dataclass
class Subset(beam.PTransform):
    """Custom PTransform to select two days and single variable"""

    def _subset(self, ds: xr.Dataset) -> xr.Dataset:
        # ds has a single 'deptht' level. For pyramids we need 3d
        ds = ds.isel(deptht=0)

        return ds

    def expand(self, pcoll):
        return pcoll | "subset" >> beam.MapTuple(lambda k, v: (k, self._subset(v)))


pyramid = (
    beam.Create(pattern.items())
    | OpenWithXarray(file_type=FileType("zarr"), xarray_open_kwargs={"chunks": {}})
    | Subset()
    | StoreToPyramid(
        store_name="eNATL60_BLBT02_pyramid.zarr",
        epsg_code="4326",
        pyramid_method="resample",
        pyramid_kwargs={"x": "x", "y": "y"},
        levels=levels,
        combine_dims=pattern.combine_dim_keys,
    )
    | ConsolidateMetadata()
    | Copy(target=catalog_store_urls["enatl60-blbt02-pyramid"])
)
