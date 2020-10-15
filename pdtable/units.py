"""Unit conversion tooling"""
from typing import Union


class DefaultUnitConverter:
    """A thin callable-class wrapper around Pint.

    Subclass to customize. Or use as is if Pint's unit registry does it for you.

    Only one instance of this class should be created, to avoid re-creating multiple instances
    of the pint unit registry. It is recommended to use the module-level singleton instead; or
    instantiate your own singleton if subclassing.
    """

    def __init__(self):
        # Check for optional requirement 'pint'
        try:
            import pint
        except ImportError as err:
            raise ImportError(
                "Unable to import 'pint'. Please install 'pint' to use the default unit converter."
            ) from err

        # Initialize unit registry once, keep it around for multiple calls
        self.ureg = pint.UnitRegistry()

    def __call__(
        self, value: float, from_unit: Union[str, "pint.Unit"], to_unit: Union[str, "pint.Unit"]
    ) -> float:
        """Converts value from one unit to another unit.

        Both units must be defined in Pint's unit registry.

        Args:
            value: Magnitude of quantity to convert
            from_unit: Unit of quantity to convert.
            to_unit: Unit to which to convert.

        Returns:
            The converted quantity's magnitude.

        """
        return self.ureg.Quantity(value, from_unit).to(to_unit).magnitude


# Singleton, for convenience.
convert_units = DefaultUnitConverter()
