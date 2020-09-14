pushd %~dp0\..
python setup.py sdist
twine upload dist/*
popd
