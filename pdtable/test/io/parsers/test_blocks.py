from pdtable.io.parsers.blocks import _safe_strip


class TestSafeStrip:
    def test_string_input(self) -> None:
        assert _safe_strip("  hello  ") == "hello"
    
    def test_int_input(self) -> None:
        assert _safe_strip(123) == "123"
        
    def test_string_input_with_leading_trailing_spaces(self) -> None:
        assert _safe_strip("  hello world  ") == "hello world"
        
    def test_int_input_with_leading_trailing_spaces(self) -> None:
        assert _safe_strip("  123  ") == "123"
