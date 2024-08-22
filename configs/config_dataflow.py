# This assumes that runner is called from a github action
# where these environment variables are set.
import os
repo_path = os.environ['GITHUB_REPOSITORY']
FEEDSTOCK_NAME = repo_path.split('/')[-1]

c.Bake.prune = True
c.Bake.bakery_class = "pangeo_forge_runner.bakery.dataflow.DataflowBakery"
c.DataflowBakery.use_dataflow_prime = False
c.DataflowBakery.machine_type = "e2-highmem-8" # 1 year had max 50GB of ram on single worker. This is 64GB
c.DataflowBakery.disk_size_gb = 400
c.DataflowBakery.max_num_workers = 1
c.DataflowBakery.use_public_ips = True
c.DataflowBakery.service_account_email = (
    "leap-community-bakery@leap-pangeo.iam.gserviceaccount.com"
)
c.DataflowBakery.project_id = "leap-pangeo"
c.DataflowBakery.temp_gcs_location = f"gs://leap-scratch/data-library/feedstocks/temp/{FEEDSTOCK_NAME}"
c.TargetStorage.fsspec_class = "gcsfs.GCSFileSystem"
c.InputCacheStorage.fsspec_class = "gcsfs.GCSFileSystem"
c.TargetStorage.root_path = f"gs://leap-scratch/data-library/feedstocks/output/{FEEDSTOCK_NAME}/{{job_name}}"
c.InputCacheStorage.root_path = f"gs://leap-scratch/data-library/feedstocks/cache"
