from setuptools import find_packages, setup

setup(
    name="xbeam_virtualizarr",
    packages=find_packages(
        exclude=["configs", "configs.*", "feedstock", "feedstock.*"]
    ),
)
