import os
from setuptools import find_packages, setup

setup(
    name='conda-deployed-app',
    version=os.environ.get('PKG_VERSION'),
    packages=find_packages(),
)
