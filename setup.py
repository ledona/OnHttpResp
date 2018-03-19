from setuptools import setup, find_packages

setup(
    name='onhttpresp',
    version='0.1',
    description='Lightweight abstraction on top of requests',
    packages=find_packages(),

    install_requires=(
        'tqdm',
        'requests',
        'sqlalchemy',
        'pytz',
    )
)
