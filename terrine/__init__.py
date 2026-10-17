"""Terrine dataset tooling.

The package currently exposes only its implemented window and error contracts.
"""

from terrine.errors import TerrineError
from terrine.windows import Window

__all__ = ["Window", "TerrineError"]
