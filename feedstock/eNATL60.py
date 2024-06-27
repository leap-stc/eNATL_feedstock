"""
...
"""
import logging
import xarray as xr
import apache_beam as beam
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    Indexed,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    T,
)

logger = logging.getLogger(__name__)

# Common Parameters
days = range(1, 32)
dataset_url = 'https://zenodo.org/records/10513552/files'

## Monthly version
input_urls = [f'{dataset_url}/eNATL60-BLBT02_y2009m07d{d:02d}.1d_TSWm_60m.nc' for d in days]
pattern = pattern_from_file_sequence(input_urls, concat_dim='time')


# does this succeed with all coords stripped?
class Preprocess(beam.PTransform):
    @staticmethod
    def _set_coords(item: Indexed[T]) -> Indexed[T]:
        index, ds = item
        logger.info(f"Index is {index=}")
        logger.info(f"Dataset before processing {ds=}")
        logger.info(f"Time counter data : {ds.time_counter.data}")
        # could try using cftime to force
        # create t_new as variable 
        t_new = xr.DataArray(ds.time_counter.data, dims=['time'])
        logger.info(f"New Time Dimension {t_new=}")
        ds = ds.assign_coords(time=t_new)
        ds = ds.drop(['time_counter'])
        ds = ds.set_coords(['deptht', 'depthw', 'nav_lon', 'nav_lat', 'tmask'])

        return index, ds

        # ds = ds.set_coords(['deptht', 'depthw', 'nav_lon', 'nav_lat', 'time_counter', 'tmask'])
        # ds = ds.assign_coords(
        #     tmask=ds.coords['tmask'].squeeze(), deptht=ds.coords['deptht'].squeeze()
        # )


    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | 'Set coordinates' >> beam.Map(self._set_coords)


eNATL60_BLBT02 = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(xarray_open_kwargs = {'use_cftime':True})
    | Preprocess()
    | StoreToZarr(
        store_name='eNATL60_BLBT02.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'x':2000, 'y':2000, 'time':2},
    )
)