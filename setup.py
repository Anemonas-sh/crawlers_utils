from setuptools import setup, find_packages

setup(
    name='crawlers_utils',
    version='1.0.0',
    description='Datatour crawlers utils library',
    packages=find_packages(),
    install_requires=[
        'google-api-python-client==1.8.3',
        'google-cloud-storage==1.29.0',
    ],
)