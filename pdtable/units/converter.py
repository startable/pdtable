"""Default unit converter"""

try:
    from ._pint import PintUnitConverter as DefaultUnitConverter
except ImportError as err:
    raise ImportError(
        "Unable to import 'pint'. Please install 'pint' to use the default unit converter."
    ) from err

# Singleton, for convenience.
convert_units = DefaultUnitConverter()
