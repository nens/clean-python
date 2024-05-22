from unittest.mock import Mock

import pytest
from fastapi import Depends
from fastapi import params
from fastapi.routing import APIRouter

from clean_python.fastapi import APIVersion
from clean_python.fastapi import get
from clean_python.fastapi import Resource
from clean_python.fastapi import Stability
from clean_python.fastapi import v


def test_subclass():
    class Cls(Resource, version=v(1), name="foo"):
        pass

    for obj in (Cls, Cls()):
        assert obj.name == "foo"
        assert obj.version == v(1)


def test_get_router_no_endpoints():
    class Cls(Resource, version=v(1)):
        pass

    router = Cls().get_router(v(1), auth_scheme=None)
    assert isinstance(router, APIRouter)
    assert len(router.routes) == 0


def test_get_router_other_version():
    class TestResource(Resource, version=v(1), name="testing"):
        @get("/foo/{id}")
        def get_test(self, id: int):
            return "ok"

    with pytest.raises(AssertionError):
        TestResource().get_router(v(2), auth_scheme=None)


def test_get_router():
    class TestResource(Resource, version=v(1), name="testing"):
        @get("/foo/{id}")
        def get_test(self, id: int):
            return "ok"

    resource = TestResource()

    router = resource.get_router(v(1), auth_scheme=None)

    assert len(router.routes) == 1

    route = router.routes[0]
    assert route.path == "/foo/{id}"
    assert route.operation_id == "get_test"
    assert route.name == "v1/get_test"
    assert route.tags == ["testing"]
    assert route.methods == {"GET"}
    assert len(route.dependencies) == 0
    # 'self' is missing from the parameters
    assert list(route.param_convertors.keys()) == ["id"]


def test_get_openapi_tag():
    class Cls(Resource, version=v(1), name="foo"):
        """Docstring"""

    actual = Cls().get_openapi_tag()

    assert actual.name == "foo"
    assert actual.description == "Docstring"


def test_v():
    assert v(1, "alpha") == APIVersion(version=1, stability=Stability.ALPHA)


@pytest.mark.parametrize(
    "version,expected",
    [(v(1, "beta"), "v1-beta"), (v(2), "v2"), (v(3, "alpha"), "v3-alpha")],
)
def test_api_version_prefix(version, expected):
    assert version.prefix == expected


def test_url_path_for():
    class TestResource(Resource, version=v(1), name="testing"):
        @get("/foo/{id}")
        def get_test(self, id: int):
            return "ok"

    resource = TestResource()
    router = resource.get_router(v(1), auth_scheme=None)

    assert router.url_path_for("v1/get_test", id=2) == "/foo/2"


def test_with_version():
    class TestResource(Resource, version=v(1), name="testing"):
        """Foo"""

        @get("/foo/{id}")
        def get_test(self, id: int):
            return "ok"

    resource_cls = TestResource.with_version(v(1, "beta"))

    assert resource_cls.version == v(1, "beta")
    assert resource_cls.name == "testing"
    assert resource_cls.__doc__ == "Foo"
    assert resource_cls.__bases__ == (TestResource,)


def test_get_less_stable():
    class V1(Resource, version=v(1), name="testing"):
        pass

    class V1Beta(V1, version=v(1, "beta"), name="testing"):
        pass

    resources = {x.version: x() for x in [V1, V1Beta]}
    assert resources[v(1)].get_less_stable(resources) is resources[v(1, "beta")]

    v1_alpha = resources[v(1, "beta")].get_less_stable(resources)
    assert v1_alpha.version == v(1, "alpha")
    assert v1_alpha.__class__.__bases__ == (V1Beta,)


def test_get_less_stable_no_subclass():
    class V1(Resource, version=v(1), name="testing"):
        pass

    class V1Beta(Resource, version=v(1, "beta"), name="testing"):
        pass

    resources = {x.version: x() for x in [V1, V1Beta]}

    with pytest.raises(RuntimeError):
        resources[v(1)].get_less_stable(resources)


TestDepends = Depends(lambda: True)


def test_get_router_auth_scheme():
    class TestResource(Resource, version=v(1), name="testing"):
        @get("/foo/{id}")
        def get_test(self, id: int):
            return "ok"

    resource = TestResource()
    auth_scheme = Mock()

    router = resource.get_router(v(1), auth_scheme=auth_scheme, responses={})

    (actual,) = router.routes[0].dependencies
    assert isinstance(actual, params.Security)
    assert actual.dependency is auth_scheme
    assert actual.scopes == []


def test_get_router_auth_dependencies_extend():
    class TestResource(Resource, version=v(1), name="testing"):
        @get("/foo/{id}", dependencies=[TestDepends])
        def get_test(self, id: int):
            return "ok"

    resource = TestResource()
    auth_scheme = Mock()

    router = resource.get_router(v(1), auth_scheme=auth_scheme, responses={})

    dep1, dep2 = router.routes[0].dependencies
    assert dep1 is TestDepends
    assert isinstance(dep2, params.Security)


def test_get_router_with_scope():
    class TestResource(Resource, version=v(1), name="testing"):
        @get("/foo/{id}", scope="foo")
        def get_test(self, id: int):
            return "ok"

    resource = TestResource()
    auth_scheme = Mock()

    router = resource.get_router(v(1), auth_scheme=auth_scheme, responses={})

    (actual,) = router.routes[0].dependencies
    assert actual.scopes == ["foo"]


def test_get_router_with_multiple_scopes():
    class TestResource(Resource, version=v(1), name="testing"):
        @get("/foo/{id}", scope=["foo", "bar"])
        def get_test(self, id: int):
            return "ok"

    resource = TestResource()
    auth_scheme = Mock()

    router = resource.get_router(v(1), auth_scheme=auth_scheme, responses={})

    (actual,) = router.routes[0].dependencies
    assert actual.scopes == ["foo", "bar"]


def test_get_router_public_with_scope():
    with pytest.raises(ValueError):
        get("/foo/{id}", public=True, scope="foo")


def test_get_router_public():
    class TestResource(Resource, version=v(1), name="testing"):
        """Foo"""

        @get("/foo", public=True)
        def foo(self):
            return "ok"

    resource = TestResource()

    router = resource.get_router(v(1), auth_scheme=Mock(), responses={})

    assert router.routes[0].dependencies == []
