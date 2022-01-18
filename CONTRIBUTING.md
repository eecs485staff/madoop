Contributing to Madoop
======================

## Install development environment
Set up a development virtual environment.
```console
$ python3 -m venv .venv
$ source env/bin/activate
$ pip install --editable .[dev,test]
```

A `madoop` entry point script is installed in your virtual environment.
```console
$ which madoop
/Users/awdeorio/src/mailmerge/.venv/bin/madoop
```

## Testing and code quality
Run unit tests
```console
$ pytest
$ pytest -vv --log-cli-level=DEBUG  # More output
```

Measure unit test case coverage
```console
$ pytest --cov ./madoop --cov-report term-missing
```

Test code style
```console
$ pycodestyle madoop tests setup.py
$ pydocstyle madoop tests setup.py
$ pylint madoop tests setup.py
$ check-manifest
```

Run linters and tests in a clean environment.  This will automatically create a temporary virtual environment.
```console
$ tox -e py3
```

## Release procedure
Update your local `develop` branch.  Make sure it's clean.
```console
$ git fetch
$ git checkout develop
$ git rebase
$ git status
```

Test
```console
$ tox -e py3
```

Update version
```console
$ $EDITOR setup.py
$ git commit -m "version bump" setup.py
$ git push origin develop
```

Update main branch
```console
$ git fetch
$ git checkout main
$ git rebase
$ git merge --no-ff origin/develop
```

Tag a release
```console
$ git tag -a X.Y.Z
$ grep version= setup.py
    version="X.Y.Z",
$ git describe
X.Y.Z
$ git push --tags origin main
```

Create a release on GitHub using the "Auto-generate release notes" feature. https://github.com/eecs485staff/madoop/releases/new

Upload to PyPI
```console
$ python3 setup.py sdist bdist_wheel
$ twine upload --sign dist/*
```
