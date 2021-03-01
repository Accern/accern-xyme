"""
Created on 2019-12-09
"""
import os
import codecs

from setuptools import setup

here = os.path.abspath(os.path.dirname(__file__))

with codecs.open(os.path.join(here, "README.rst"), encoding="utf-8") as f:
    long_description = f.read()

# NOTE! steps to distribute:
# $ make publish

setup(
    name="accern_xyme",
    version="0.2.0",
    description=(
        "AccernXYME is a library for easily "
        "accessing XYME via python."),
    long_description=long_description,
    url="https://github.com/Accern/accern-xyme",
    author="Accern Corp.",
    author_email="josua.krause@accern.com",
    license="MIT",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3",
    ],
    keywords="XYME AI machine learning client",
    packages=["accern_xyme"],
    install_requires=[
        "numpy>=1.17.3",
        "pandas>=0.25.3",
        "quick-server>=0.7.14",
        "requests>=2.22.0",
        "RestrictedPython>=5.0",
        "scipy>=1.4.1",
        "torch>=1.5.1"
        "typing-extensions>=3.7.4.1",
    ],
    extras_require={
        "dev": [],
        "test": [],
    },
    package_data={
        'accern_xyme': [
            'py.typed',
        ],
    },
    data_files=[],
    python_requires=">=3.6",
)
