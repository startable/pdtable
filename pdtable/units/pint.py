"""A courtesy pint-based unit converter.

Here we define
- a callable-class wrapper around Pint
- a singleton instance of this class, for convenient use and reuse
"""

from typing import Union, Tuple


class PintUnitConverter:
    """A thin callable-class wrapper around Pint.

    Subclass to customize. Or use as-is if Pint's unit registry does it for you.

    This class should be instantiated only once and used as a singleton, to avoid creating multiple,
    unrelated instances of the pint unit registry.
    """

    def __init__(self):
        self.ureg = None  # Placeholder for pint unit registry

    def __call__(
        self,
        value: float,
        from_unit: Union[str],
        to_unit: Union[str] = "__base_unit__",
    ) -> Tuple[float, str]:
        """Converts value from one unit to another unit.

        Both units must be defined in Pint's unit registry.

        If to_unit is not specified, it defaults to from_unit's base unit.
        For example, converting 2 'cm' will return 0.02 'meter'.

        Args:
            value: Magnitude of quantity to convert
            from_unit: Unit of quantity to convert.
            to_unit: Unit to which to convert.  If None (default), converts to base unit.

        Returns:
            The converted quantity's magnitude and unit. Note that if to_unit is specified,
            then the returned unit is pint's string representation of to_unit.
            If, however, to_unit is not specified, then the returned unit is pint's string
            representation of from_unit's base unit.

        """
        try:
            import pint
        except ImportError as err:
            raise ImportError(
                "Unable to import 'pint'. "
                "Please install 'pint' to use this pint-based unit converter."
            ) from err

        # Initialize unit registry once, keep it around for multiple calls
        if self.ureg is None:
            self.ureg = pint.UnitRegistry()

        if str(to_unit) == str(from_unit):
            # Null conversion
            return value, str(from_unit)
        elif to_unit == "__base_unit__":
            # Convert to base unit
            converted_quantity = self.ureg.Quantity(value, from_unit).to_base_units()
        else:
            # Convert to specified unit
            converted_quantity = self.ureg.Quantity(value, from_unit).to(to_unit)

        return converted_quantity.magnitude, str(converted_quantity.units)


# Singleton
pint_converter = PintUnitConverter()