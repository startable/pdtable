"""A pint-based unit converter."""

from typing import Union

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
        self, value: float, from_unit: Union[str, pint.Unit], to_unit: Union[str, pint.Unit]
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
