"""Setup shim for setuptools.

Project metadata lives in ``pyproject.toml`` (``[project]`` tables): name,
version, description, readme, license (MIT, ``LICENSE``), classifiers,
dependencies, optional dev extras, URLs, and package/package-data layout
(including the PEP 561 ``py.typed`` marker under ``[tool.setuptools]``).

This file exists so legacy ``python setup.py`` invocations still work. It
intentionally declares nothing itself -- defining metadata here would conflict
with the single source of truth in ``pyproject.toml``.
"""

from setuptools import setup

setup()
