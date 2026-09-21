"""Package version (single source of truth for packaging and introspection).

Importing this module must not import anything else, so ``setup.py`` can
``exec`` it before any dependencies are installed.
"""

from __future__ import annotations

__version__: str = "0.1.0"
__version_info__: tuple[int, int, int] = (0, 1, 0)
