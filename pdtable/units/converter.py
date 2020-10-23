"""Default unit converter"""

try:
    from ._pint import PintUnitConverter as DefaultUnitConverter
    # Singleton, for convenience.
    pint_converter = DefaultUnitConverter()

except ImportError as err:
    raise ImportError(
        "Unable to import 'pint'. Please install 'pint' to use the default unit converter."
    ) from err

