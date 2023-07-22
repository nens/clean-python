import pytest

from clean_python.fastapi import Resource
from clean_python.fastapi import Service
from clean_python.fastapi import v


class V1Foo(Resource, version=v(1), name="foo"):
    pass


class V1BetaFoo(V1Foo, version=v(1, "beta"), name="foo"):
    pass


class V1AlphaFoo(V1BetaFoo, version=v(1, "alpha"), name="foo"):
    pass


class V2AlphaFoo(Resource, version=v(2, "alpha"), name="foo"):
    pass


@pytest.mark.parametrize(
    "resource_classes",
    [
        (V1AlphaFoo,),
        (V1BetaFoo, V1AlphaFoo),
        (V1Foo, V1BetaFoo, V1AlphaFoo),
        (V1AlphaFoo, V2AlphaFoo),
    ],
)
def test_service_init(resource_classes):
    resources = [cls() for cls in resource_classes]
    service = Service(*resources)
    assert set(service.resources) == set(resources)


@pytest.mark.parametrize(
    "resource_classes,expected_versions",
    [
        ((V1BetaFoo,), {v(1, "beta"), v(1, "alpha")}),
        ((V1Foo,), {v(1), v(1, "beta"), v(1, "alpha")}),
    ],
)
def test_service_init_dynamic_gen(resource_classes, expected_versions):
    resources = [cls() for cls in resource_classes]
    service = Service(*resources)
    assert set(x.version for x in service.resources) == expected_versions
