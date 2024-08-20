import apache_beam as beam
import xarray as xr
import numpy as np
import xesmf as xe
from pangeo_forge_recipes.transforms import OpenWithXarray
from pangeo_forge_recipes.patterns import FileType, pattern_from_file_sequence
from dataclasses import dataclass
from leap_data_management_utils.data_management_transforms import (
    get_catalog_store_urls,
)

# parse the catalog store locations (this is where the data is copied to after successful write (and maybe testing)
catalog_store_urls = get_catalog_store_urls("feedstock/catalog.yaml")


# How many pyramid levels
# https://agupubs.onlinelibrary.wiley.com/doi/full/10.1029/2023MS003959
# spatial resolution from ds.attrs seems to be 1/60 degree or at equator (~111000 meters * 1/60)
# import morecantile
# tms = morecantile.tms.get("WebMercatorQuad")

# lvls = tms.zoom_for_res(111000.0/60.0)
# > 6
levels = 6


pattern = pattern_from_file_sequence(
    [catalog_store_urls["enatl60-blbt02"]],
    concat_dim="time",
)


@dataclass
class GenerateWeights(beam.PTransform):
    """Custom PTransform to generate weights for xESMF regridding"""

    def _generate_weights(self, ds: xr.Dataset) -> xr.Dataset:
        ds = ds.rio.write_crs("EPSG:4326")
        # grab sample of dataset for weights
        nds = ds.isel(time=0)[["vosaline"]]

        lat_min, lat_max = nds.nav_lat.min().values, nds.nav_lat.max().values
        lon_min, lon_max = nds.nav_lon.min().values, nds.nav_lon.max().values

        # dims at level 6
        lat = np.linspace(lat_min, lat_max, 4096)
        lon = np.linspace(lon_min, lon_max, 4096)

        ds_out = xr.Dataset(
            coords={"lat": ("lat", lat), "lon": ("lon", lon)},
            data_vars={
                "mask": (["lat", "lon"], np.ones((len(lat), len(lon)), dtype=bool))
            },
        )
        weights_local_filename = "enatl_weights_4096.nc"
        regridder = xe.Regridder(ds, ds_out, "bilinear", weights=weights_local_filename)
        weights_ds = xr.open_dataset("weights_local_filename")
        weights_ds.to_netcdf(
            f"gs://leap-scratch/data-library/feedstocks/eNATL_regridding/{weights_local_filename}"
        )
        weights_ds.to_zarr(
            "gs://leap-scratch/data-library/feedstocks/eNATL_regridding/enatl_weights_4096.zarr"
        )

        return weights_ds

    def expand(self, pcoll):
        return pcoll | "subset" >> beam.MapTuple(lambda k, v: (k, self._subset(v)))


pyramid = (
    beam.Create(pattern.items())
    | OpenWithXarray(file_type=FileType("zarr"), xarray_open_kwargs={"chunks": {}})
    | GenerateWeights()
    # | StoreToPyramid(
    #     store_name="eNATL60_BLBT02_pyramid.zarr",
    #     epsg_code="4326",
    #     pyramid_method="resample",
    #     pyramid_kwargs={"x": "x", "y": "y"},
    #     levels=levels,
    #     combine_dims=pattern.combine_dim_keys,
    # )
    # | ConsolidateMetadata()
    # | Copy(target=catalog_store_urls["enatl60-blbt02-pyramid"])
)
