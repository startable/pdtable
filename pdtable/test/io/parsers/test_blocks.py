import datetime
import warnings
from pdtable.io.parsers.blocks import _get_destinations_safely_stripped


class TestGetDestinationsSafelyStripped:
    def test_string_input(self) -> None:
        assert _get_destinations_safely_stripped("  hello  ") == "hello"
    
    def test_int_input(self) -> None:
        assert _get_destinations_safely_stripped(123) == "123"
        
    def test_string_input_with_leading_trailing_spaces(self) -> None:
        assert _get_destinations_safely_stripped("  hello world  ") == "hello world"
        
    def test_int_input_with_leading_trailing_spaces(self) -> None:
        assert _get_destinations_safely_stripped("  123  ") == "123"

    def test_datetime_input(self) -> None:
        datetime_now = datetime.datetime.now()
        
        with warnings.catch_warnings(record=True) as w:
            destinations = _get_destinations_safely_stripped(datetime_now)
            assert len(w) == 1
            assert destinations.replace(' ', '') == destinations
