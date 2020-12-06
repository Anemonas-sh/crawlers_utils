from setuptools import setup, find_packages

setup(
    name='crawlers_utils',
    version='1.0.1',
    description='Datatour crawlers utils library',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'google-api-python-client==1.8.3',
        'google-cloud-storage==1.29.0',
        'pyvirtualdisplay==1.3.2',
        'selenium==3.141.0'
    ],
)
