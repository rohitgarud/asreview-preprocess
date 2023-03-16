# based on https://github.com/pypa/sampleproject
# MIT License

from io import open
from os import path

# Always prefer setuptools over distutils
from setuptools import find_namespace_packages, setup

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
    install_requires=[
        "asreview>=1,<2",
        "unidecode",
        "tinydb",
        "pyalex",
        "recordlinkage",
    ],
    extras_require={},
    entry_points={
        "asreview.entry_points": [
            "preprocess = asreviewcontrib.preprocess.entry_points.entrypoint:PreprocessEntryPoint",
        ],
        "asreview.preprocess.entry_points": [],
        "asreview.preprocess.updaters": [
            "openalex = asreviewcontrib.preprocess.update_data.openalex_updater:OpenAlexUpdater",
        ],
        "asreview.preprocess.localdbs": [
            "tinydb = asreviewcontrib.preprocess.local_db.tinylocaldb:TinyLocalDB",
        ],
        "asreview.preprocess.deduplicators": [
            "asr = asreviewcontrib.preprocess.deduplication.methods.asr:ASRDedup",
            "asr = asreviewcontrib.preprocess.deduplication.methods.endnote_default:ENDefaultDedup",
        ],
    },
    project_urls={
        "Bug Reports": "https://github.com/rohitgarud/asreview-preprocess/issues",
        "Source": "https://github.com/rohitgarud/asreview-preprocess",
    },
)
