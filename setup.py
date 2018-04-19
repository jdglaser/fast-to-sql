# -*- coding: utf-8 -*-
"""
Created on Thu Apr 19 10:03:10 2018

@author: jglasej
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.dirname(__file__)

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()
    
setup(
    name='fast_to_SQL',
    version='1.0.1',
    description='An improved way of uploading pandas DataFrames to MS SQL server',
    long_description=long_description,
    url='https://github.com/jdglaser/fast_to_SQL',
    author=['Jarred Glaser'],
    author_email='jarred.glaser@gmail.com',
    license='MIT License',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3.6',
        'License :: MIT License'
        ],
    install_requires=["pandas", "sqlalchemy", 'datetime'],
    keywords='pandas to_sql fast sql',
    packages=['fast_to_SQL'],
)
