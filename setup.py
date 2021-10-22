"""Michigan Hadoop CLI build and install configuration."""
import os
import io
import setuptools


# Read the contents of README file
PROJECT_DIR = os.path.abspath(os.path.dirname(__file__))
with io.open(os.path.join(PROJECT_DIR, "README.md"), encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()


setuptools.setup(
    name="madoop",
    description="An lightweight, easy to use, Python based Hadoop CLI",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    version="0.1.0",
    author="Andrew DeOrio",
    author_email="awdeorio@umich.edu",
    url="https://github.com/eecs485staff/michigan-hadoop/",
    license="MIT",
    packages=["madoop"],
    keywords=[
        "madoop", "michigan hadoop",
    ],
    install_requires=[],
    extras_require={
        "dev": [
            "pdbpp",
            "twine",
            "tox",
        ],
        "test": [
            "check-manifest",
            "freezegun",
            "pycodestyle",
            "pydocstyle",
            "pylint",
            "pytest",
            "pytest-cov",
            "pytest-mock",
            "requests-mock",
        ],
    },
    python_requires='>=3.6',
    entry_points={
        "console_scripts": [
            "madoop = madoop.__main__:main",
        ]
    },
)
