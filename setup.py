
from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    f_license = f.read()

setup(
    name='agonyless',
    version='0.0.1',
    description='Tool to interact with Adaptiv Networks 7.X CPEs',
    long_description=readme,
    author='sjackson',
    author_email='sjackson@adaptiv-networks.com',
    url='https://github.com/TeloipInc/AgonyLess',
    license=f_license,
    packages=find_packages(),
    install_requires=['paramiko', 'pyyaml', 'colorama']
)
