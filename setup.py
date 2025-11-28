from setuptools import find_packages, setup

setup(
    name="onhttpresp",
    version="2025.11.28",
    description="Lightweight abstraction on top of requests",
    packages=find_packages(),
    install_requires=("tqdm", "requests", "sqlalchemy", "pytz", "python-dateutil"),
)
