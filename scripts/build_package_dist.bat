pushd %~dp0\..
python setup.py sdist
REM twine upload dist/*
popd
