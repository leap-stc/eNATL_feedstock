import pandas as pd
import pooch
import subprocess
import os
from pangeo_forge_recipes.patterns import pattern_from_file_sequence

import apache_beam as beam
from apache_beam.io.gcp.gcsio import GcsIO
import xarray as xr
import io


# grab from recipes
import logging

logger = logging.getLogger(__name__)

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


flist = [make_full_path(time) for time in dates]
# flist = flist[0:2]
pattern = pattern_from_file_sequence(flist, concat_dim="time")


class DownloadAndTransfer(beam.DoFn):
    def process(self, url, bucket):
        # unpack filepattern tuple
        url = url[1]
        # grab last bit of filename
        filename = url.split("/")[-1]

        # check if file exists already, if does, skip
        file_exists_cmd = f"s5cmd --endpoint-url https://storage.googleapis.com head 's3://{bucket}/{filename}'"
        file_exists = subprocess.run(
            file_exists_cmd, shell=True, capture_output=True, text=True
        )
        if file_exists:
            # should this be logging
            logger.debug(f"{filename} already exists")
            return

        local_path = pooch.retrieve(url=url, known_hash=None)
        gcs_path = f's3://{bucket}/{filename}'

        command = f"s5cmd --endpoint-url https://storage.googleapis.com cp {local_path} '{gcs_path}'"
        subprocess.run(command, shell=True, capture_output=True, text=True)

        # Clean up the local file
        os.remove(local_path)
        return gcs_path


# Define and run the pipeline
bucket = "leap-scratch/norlandrhagen/pooch"

poochpipeline = beam.Create(pattern.items()) | "pooch and s5cmd" >> beam.ParDo(
    DownloadAndTransfer(), bucket=bucket
) | beam.Map(print)






class OpenXarrayBytes(beam.DoFn):
    def process(self, gcs_path):
        gcs = GcsIO()
        with gcs.open(gcs_path) as f:
            file_contents = f.read()
        
        bytes_io = io.BytesIO(file_contents)
        ds = xr.open_dataset(bytes_io)
        
        

with beam.Pipeline() as p:
    (p 
     | beam.Create([gcs files])
     | beam.ParDo(OpenXarrayBytes())    )