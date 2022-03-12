"""Setup script for fast_to_sql
"""

import os
# Always prefer setuptools over distutils
import sys

from setuptools import find_packages, setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def main():
    """Executes setup when this script is at the top-level
    """
    import fast_to_sql as app

    setup(
        name=app.__project__,
        version=app.__version__,
        description=app.__doc__,
        long_description=read("README.md"),
        long_description_content_type='text/markdown',
        classifiers=app.__classifiers__,
        author=app.__author__,
        author_email=app.__author_email__,
        url=app.__url__,
        license=[
            c.rsplit('::', 1)[1].strip()
            for c in app.__classifiers__
            if c.startswith('License ::')
        ][0],
        keywords=app.__keywords__,
        packages=find_packages(),
        include_package_data=True,
        platforms=app.__platforms__,
        install_requires=app.__requires__,
        extras_require=app.__extra_requires__,
    )


if __name__ == '__main__':
    main()
