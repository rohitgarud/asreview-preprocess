# based on https://github.com/pypa/sampleproject
# MIT License

from io import open
from os import path

# Always prefer setuptools over distutils
from setuptools import find_namespace_packages
from setuptools import setup

import versioneer

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="asreview-preprocess",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="ASReview preprocessing extension for preprocessing records from dataset including deduplication, finding missing abstracts",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rohitgarud/asreview-preprocess",
    author="Rohit Garud",
    author_email="rohit.garuda1992@gmail.com",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    keywords="asreview preprocess",
    packages=find_namespace_packages(include=["asreviewcontrib.*"]),
    install_requires=["asreview>=1,<2", "unidecode", "tinydb"],
    extras_require={
        "abstract_finder": ["pyalex"],
        "gui": [
            "flask-SQLalchemy-3.0.3",
            "flask-bootstrap",
            "flask-wtf",
        ],
        "all": [
            "pyalex",
            "flask-SQLalchemy-3.0.3",
            "flask-bootstrap",
            "flask-wtf",
        ],
    },
    entry_points={
        "asreview.entry_points": [
            "preprocess = asreviewcontrib.preprocess.entry_points.entrypoint:PreprocessEntryPoint",
        ]
    },
    project_urls={
        "Bug Reports": "https://github.com/rohitgarud/asreview-preprocess/issues",
        "Source": "https://github.com/rohitgarud/asreview-preprocess",
    },
)
