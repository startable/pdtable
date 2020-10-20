"""Checks whether the demo examples run without crashing."""


def test_pdtable_demo():
    # Just import the jupytext script to run it
    import examples.pdtable_demo  # noqa: F401
