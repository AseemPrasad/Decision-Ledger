"""Decision Ledger packaging (setup.py).

Mirror of the authoritative ``pyproject.toml`` ``[project]`` metadata kept so
legacy ``python setup.py`` invocations behave identically to ``pip install .``
and report the same name/version/dependencies. setuptools always prefers the
PEP 621 metadata from ``pyproject.toml`` on the build path, so the two files
must agree; please keep them in sync when releasing.

The version is a single source of truth: ``pyproject.toml`` reads it from
``decision_ledger/__version__.py`` via ``[tool.setuptools.dynamic]`` and this
file imports the same value, so bump it in exactly one place.
"""

import os
from pathlib import Path

from setuptools import find_packages, setup

_ROOT = Path(__file__).resolve().parent.parent
_HERE = Path(__file__).resolve().parent

os.chdir(_ROOT)  # resolve all relative globs (src/, LICENSE, find_packages) to the project root

_version: dict = {}
exec((_HERE / "decision_ledger" / "__version__.py").read_text(encoding="utf-8"), _version)

setup(
    name="decision-ledger",
    version=_version["__version__"],
    description="Statistical trust gate for small-model control-plane delegation",
    long_description=(_ROOT / "README.md").read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    author="Decision Ledger",
    license="MIT",
    license_files=("LICENSE",),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
    keywords="llm conformal calibration control-plane ml-systems".split(),
    python_requires=">=3.8",
    packages=find_packages("src"),
    package_dir={"": "src"},
    package_data={"decision_ledger": ["py.typed"]},
    install_requires=[
        "numpy>=1.24.0",
        "PyYAML>=6.0",
        "blake3>=1.0.0",
        "uuid6>=2024.0.0",
    ],
    extras_require={
        "dev": [
            "build>=1.0.0",
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "pytest-benchmark>=4.0.0",
            "black>=23.0.0",
            "isort>=5.12.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
        "test": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "decision-ledger-outcome = decision_ledger.outcomes:main",
        ],
    },
    project_urls={
        "Documentation": "https://github.com/example/decision-ledger/tree/main/src/docs",
        "Changelog": "https://github.com/example/decision-ledger/blob/main/CHANGELOG.md",
        "Homepage": "https://github.com/example/decision-ledger",
    },
)
