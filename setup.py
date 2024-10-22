from setuptools import setup, find_packages

setup(
    name="xbeam_virtualizarr",
    packages=find_packages(
        exclude=["configs", "configs.*", "feedstock", "feedstock.*"]
    ),
)
