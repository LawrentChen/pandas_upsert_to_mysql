# !usr/bin/env python3
# -*- coding: utf-8 -*-

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="pandas_upsert_to_mysql",
    version="0.0.2",
    author="LawrentChen",
    author_email="laurant.chen@gmail.com",
    description="Enhanced `to_sql` method in pandas DataFrame, for MySQL database only.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/LawrentChen/pandas_upsert_to_mysql",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'pandas',
        'SQLAlchemy',
        'mysqlclient'
    ]
)

if __name__ == '__main__':
    pass