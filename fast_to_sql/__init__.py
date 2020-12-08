from __future__ import absolute_import
from .fast_to_sql import fast_to_sql

__project__ = "fast-to-sql"
__version__ = "develop"
__keywords__ = ["pandas", "to_sql", "fast", "sql"]
__author__ = "Jarred Glaser"
__author_email__ = "jarred.glaser@gmail.com"
__url__ = "https://github.com/jdglaser/fast-to-sql"
__platforms__ = "ALL"

__classifiers__ = [
    'Development Status :: 5 - Production/Stable',
    'Intended Audience :: Developers',
    'Programming Language :: Python :: 3.8',
    'License :: OSI Approved :: MIT License'
]

__requires__ = ["pandas", "pyodbc"]

__extra_requires__ = {
}