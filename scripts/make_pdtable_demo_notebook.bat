pushd %~dp0\..
jupytext examples\pdtable_demo.py --to notebook --execute
rem jupytext examples\pdtable_demo.py --execute --to markdown --update-metadata "{""jupytext"": {""notebook_metadata_filter"":""-all""}}"
rem jupyter nbconvert examples\pdtable_demo.ipynb --to notebook --execute --inplace
rem jupyter nbconvert examples\pdtable_demo.ipynb --to markdown
rem jupyter nbconvert examples\pdtable_demo.ipynb --to html
popd