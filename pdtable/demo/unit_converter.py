"""Demo unit conversion"""


def convert_this(value: float, from_unit: str, to_unit: str) -> float:
    """A simple unit converter that hasn't read a lot of books.

    This is an example of how the user could implement a unit converting function.
    Here we define a few unit conversions manually. But the user could also choose to just
    delegate this to some dedicated unit support package like 'pint' or 'unum' or whatever.
    As long as everything is wrapped in a function with the same signature as this one.
    """
    requested_conversion = (from_unit, to_unit)
    available_conversions = {
        ('m', 'mm'): lambda x: x * 1000,
        ('mm', 'm'): lambda x: x / 1000,
        ('C', 'K'): lambda x: x + 273.16,
        ('K', 'C'): lambda x: x - 273.16,
    }
    if requested_conversion not in available_conversions:
        raise KeyError(f"I don't know how to convert from '{from_unit}' to '{to_unit}'")

    return available_conversions[requested_conversion](value)
