from textwrap import dedent

from ..directives import Directive


def test_directive():
    d = Directive("foo", ["bar", "baz"])
    assert str(d) == dedent("""\
        ***foo
        bar
        baz""")