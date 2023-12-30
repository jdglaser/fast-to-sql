"""Setup script for fast_to_sql
"""

import os

from setuptools import find_packages, setup


def read(fname: str):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="fast-to-sql",
    version=os.getenv("PYPI_PACKAGE_VERSION", "v0.0.1dev0"),
    description="An improved way to upload pandas dataframes to Microsoft SQL Server.",
    long_description=read("README.md"),
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: MIT License",
    ],
    author="Jarred Glaser",
    author_email="jarred.glaser@gmail.com",
    url="https://github.com/jdglaser/fast-to-sql",
    license="License :: OSI Approved :: MIT License",
    keywords=["pandas", "to_sql", "fast", "sql"],
    packages=find_packages("fast_to_sql"),
    include_package_data=True,
    install_requires=["pandas", "pyodbc"],
)
