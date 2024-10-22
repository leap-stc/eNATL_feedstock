import argparse


def run(argv=None, save_main_session=True):
    import xarray as xr
    import apache_beam as beam
    import xarray_beam as xbeam

    from apache_beam.options.pipeline_options import PipelineOptions
    from apache_beam.options.pipeline_options import SetupOptions

    """Main entry point; defines and runs the wordcount pipeline."""
    parser = argparse.ArgumentParser()

    _, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # IO
    reference_path = "gs://leap-persistent/norlandrhagen/references/enatl_600m.parquet"

    output_path = "gs://leap-persistent/data-library/feedstocks/eNATL_feedstock/virtualizarr/eNATL600m-BLBT02.zarr"

    source_dataset = xr.open_dataset(reference_path, engine="kerchunk", chunks=None)
    template = xbeam.make_template(source_dataset)
    source_chunks = {"y": 157, "x": 8354, "time": 1}
    # target_chunks = {"time": 100, "y": 400, "x": 800}
    target_chunks = {"time": 3, "y": 2365, "x": 4177}
    itemsize = 4  # for float32

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            | xbeam.DatasetToChunks(source_dataset, source_chunks, split_vars=True)
            | xbeam.Rechunk(
                source_dataset.sizes,
                source_chunks,
                target_chunks,
                itemsize=itemsize,
                max_mem=4 * 2**30,
            )
            | xbeam.ChunksToZarr(
                store=output_path, template=template, zarr_chunks=target_chunks
            )
        )


if __name__ == "__main__":
    run()
