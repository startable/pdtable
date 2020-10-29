"""Default unit converter"""

try:
    from .pint import PintUnitConverter

    # Singleton, for convenience.
    pint_converter = PintUnitConverter()

except ImportError as err:
    raise ImportError(
        "Unable to import 'pint'. Please install 'pint' to use the default unit converter."
    ) from err
