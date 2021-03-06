"""Madoop build and install configuration."""
import pathlib
import setuptools


# Read the contents of README file
PROJECT_DIR = pathlib.Path(__file__).parent
README = PROJECT_DIR/"README.md"
LONG_DESCRIPTION = README.open(encoding="utf8").read()


setuptools.setup(
    name="madoop",
    description="A light weight MapReduce framework for education.",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    version="1.0.1",
    author="Andrew DeOrio",
    author_email="awdeorio@umich.edu",
    url="https://github.com/eecs485staff/madoop/",
    license="MIT",
    packages=["madoop"],
    package_data={"madoop": ["example/*.py", "example/input/*.txt"]},
    keywords=[
        "madoop", "Hadoop", "MapReduce", "Michigan Hadoop", "Hadoop Streaming"
    ],
    install_requires=[],
    extras_require={
        "dev": [
            "pdbpp",
            "twine",
            "tox",
            "check-manifest",
            "freezegun",
            "pycodestyle",
            "pydocstyle",
            "pylint",
            "pytest",
            "pytest-cov",
        ],
    },
    python_requires='>=3.6',
    entry_points={
        "console_scripts": [
            "madoop = madoop.__main__:main",
        ]
    },
)
