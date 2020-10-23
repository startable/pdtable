"""A pint-based unit converter."""

from typing import Union, Optional, Tuple

import pint


class PintUnitConverter:
    """A thin callable-class wrapper around Pint.

    Subclass to customize. Or use as-is if Pint's unit registry does it for you.

    This class should be instantiated only once and used as a singleton, to avoid creating multiple,
    unrelated instances of the pint unit registry.
    """

    def __init__(self):
        # Initialize unit registry once, keep it around for multiple calls
        self.ureg = pint.UnitRegistry()

    def __call__(
        self,
        value: float,
        from_unit: Union[str, pint.Unit],
        to_unit: Optional[Union[str, pint.Unit]] = None,
    ) -> Tuple[float, str]:
        """Converts value from one unit to another unit.

        Both units must be defined in Pint's unit registry.

        If to_unit is not specified (None), it defaults to from_unit's base unit.
        For example, converting 2 'cm' will return 0.02 'meter'.

        Args:
            value: Magnitude of quantity to convert
            from_unit: Unit of quantity to convert.
            to_unit: Unit to which to convert.  If None (default), converts to base unit.

        Returns:
            The converted quantity's magnitude and unit. Note that if to_unit is specified,
            then the returned unit is pint's string representation of to_unit.
            If, however, to_unit is not specified (None), then the returned unit is pint's string
            representation of from_unit's base unit.

        """
        if str(to_unit) == str(from_unit):
            # Null conversion
            return value, str(from_unit)
        elif to_unit is None:
            # Convert to base unit
            converted_quantity = self.ureg.Quantity(value, from_unit).to_base_units()
        else:
            # Convert to specified unit
            converted_quantity = self.ureg.Quantity(value, from_unit).to(to_unit)

        return converted_quantity.magnitude, str(converted_quantity.units)

    def base_unit(self, unit: Union[str, pint.Unit]) -> str:
        """Returns the base unit of the supplied unit.

        For example, base_unit('cm') returns 'meter'.

        Args:
            unit: Unit for which to find base unit.

        Returns: Base unit of supplied unit.
        """
        return str(self.ureg.get_base_units(unit)[1])  # 0th element is magnitude