pushd %~dp0\..
jupytext examples\pdtable_demo.py --to notebook
rem jupytext examples\pdtable_demo.py --execute --to markdown --update-metadata "{""jupytext"": {""notebook_metadata_filter"":""-all""}}"
jupyter nbconvert examples\pdtable_demo.ipynb --to notebook --execute --inplace
jupyter nbconvert examples\pdtable_demo.ipynb --to markdown
jupyter nbconvert examples\pdtable_demo.ipynb --to html
rem --update-metadata "{""jupytext"": {""notebook_metadata_filter"":""-all""}}"
popd