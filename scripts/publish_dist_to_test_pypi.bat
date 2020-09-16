pushd %~dp0\..
twine upload --repository testpypi dist/*
popd

ECHO To test out a pip install from testpypi:
ECHO pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple pdtable
