"""This script is used to extract the minimum requirements from the pyproject.toml,
and export them as a list of pinned requirements.

The scripts assumes that the current package is installed as 'editable', so that its
metadata can be found with importlib.

Usage:
    $ python clean_python/testing/extract_dependencies.py clean_python[nanoid,fastapi]
    pydantic==2.9 inject==5 asgiref==3.8 blinker==1.8 async-lru==2.0 backoff==2.2 pyyaml==6.0 fastapi==0.115 nanoid==2
"""

import sys
from importlib.metadata import requires

from packaging.requirements import Requirement
from packaging.specifiers import Specifier
from packaging.specifiers import SpecifierSet


def get_requirements(package_name: str, extras: set[str]) -> list[Requirement]:
    result = []
    requirements_from_importlib = requires(package_name)
    assert requirements_from_importlib, f"Package {package_name} not found"
    for v in requirements_from_importlib:
        req = Requirement(v)
        if not req.marker or any(req.marker.evaluate({"extra": e}) for e in extras):
            result.append(req)
    return result


def specifier_to_minimum(specifier: Specifier) -> Specifier:
    if specifier.operator == ">=":
        return Specifier(f"=={specifier.version}")
    else:
        return specifier


def specifier_set_to_minimum(specifier_set: SpecifierSet) -> SpecifierSet:
    return SpecifierSet(
        specifiers=",".join([str(specifier_to_minimum(s)) for s in specifier_set])
    )


def get_minimum_requirements(package_name: str, extras: set[str]) -> list[Requirement]:
    return [
        Requirement(f"{req.name}{specifier_set_to_minimum(req.specifier)}")
        for req in get_requirements(package_name, extras)
    ]


if __name__ == "__main__":
    req = Requirement(sys.argv[1])
    print("\n".join([str(r) for r in get_minimum_requirements(req.name, req.extras)]))
