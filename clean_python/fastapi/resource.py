# (c) Nelen & Schuurmans

from enum import Enum
from functools import partial
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Type

from fastapi import Depends
from fastapi.routing import APIRouter

from clean_python import ValueObject

from .security import RequiresScope

__all__ = [
    "Resource",
    "get",
    "post",
    "put",
    "patch",
    "delete",
    "APIVersion",
    "Stability",
    "v",
    "clean_resources",
]


class Stability(str, Enum):
    STABLE = "stable"
    BETA = "beta"
    ALPHA = "alpha"

    @property
    def description(self) -> str:
        return DESCRIPTIONS[self]

    def decrease(self) -> "Stability":
        index = STABILITY_ORDER.index(self)
        if index == 0:
            raise ValueError(f"Cannot decrease stability of {self}")
        return STABILITY_ORDER[index - 1]


STABILITY_ORDER = [Stability.ALPHA, Stability.BETA, Stability.STABLE]
DESCRIPTIONS = {
    Stability.STABLE: "The stable API version.",
    Stability.BETA: "Backwards incompatible changes will be announced beforehand.",
    Stability.ALPHA: "May get backwards incompatible changes without warning.",
}


class APIVersion(ValueObject):
    version: int
    stability: Stability

    @property
    def prefix(self) -> str:
        result = f"v{self.version}"
        if self.stability is not Stability.STABLE:
            result += f"-{self.stability.value}"
        return result

    @property
    def description(self) -> str:
        return self.stability.description

    def decrease_stability(self) -> "APIVersion":
        return APIVersion(version=self.version, stability=self.stability.decrease())


def http_method(path: str, scope: Optional[str] = None, **route_options):
    def wrapper(unbound_method: Callable[..., Any]):
        setattr(
            unbound_method,
            "http_method",
            (path, scope, route_options),
        )
        return unbound_method

    return wrapper


def v(version: int, stability: str = "stable") -> APIVersion:
    return APIVersion(version=version, stability=Stability(stability))


get = partial(http_method, methods=["GET"])
post = partial(http_method, methods=["POST"])
put = partial(http_method, methods=["PUT"])
patch = partial(http_method, methods=["PATCH"])
delete = partial(http_method, methods=["DELETE"])


class OpenApiTag(ValueObject):
    name: str
    description: Optional[str]


class Resource:
    version: APIVersion
    name: str

    def __init_subclass__(cls, version: APIVersion, name: str = ""):
        cls.version = version
        cls.name = name
        super().__init_subclass__()

    @classmethod
    def with_version(cls, version: APIVersion) -> Type["Resource"]:
        class DynamicResource(cls, version=version, name=cls.name):  # type: ignore
            pass

        DynamicResource.__doc__ = cls.__doc__

        return DynamicResource

    def get_less_stable(self, resources: Dict[APIVersion, "Resource"]) -> "Resource":
        """Fetch a less stable version of this resource from 'resources'

        If it doesn't exist, create it dynamically.
        """
        less_stable_version = self.version.decrease_stability()

        # Fetch the less stable resource; generate it if it does not exist
        try:
            less_stable_resource = resources[less_stable_version]
        except KeyError:
            less_stable_resource = self.__class__.with_version(less_stable_version)()

        # Validate the less stable version
        if less_stable_resource.__class__.__bases__ != (self.__class__,):
            raise RuntimeError(
                f"{less_stable_resource} should be a direct subclass of {self}"
            )

        return less_stable_resource

    def _endpoints(self):
        for attr_name in dir(self):
            if attr_name.startswith("_"):
                continue
            endpoint = getattr(self, attr_name)
            if not hasattr(endpoint, "http_method"):
                continue
            yield endpoint

    def get_openapi_tag(self) -> OpenApiTag:
        return OpenApiTag(
            name=self.name,
            description=self.__class__.__doc__,
        )

    def get_router(
        self, version: APIVersion, responses: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> APIRouter:
        assert version == self.version
        router = APIRouter()
        operation_ids = set()
        for endpoint in self._endpoints():
            path, scope, route_options = endpoint.http_method
            operation_id = endpoint.__name__
            if operation_id in operation_ids:
                raise RuntimeError(
                    "Multiple operations {operation_id} configured in {self}"
                )
            operation_ids.add(operation_id)
            # The 'name' is used for reverse lookups (request.path_for): include the
            # version prefix so that we can uniquely refer to an operation.
            name = version.prefix + "/" + endpoint.__name__
            # 'scope' is implemented using FastAPI's dependency injection system
            if scope is not None:
                route_options.setdefault("dependencies", [])
                route_options["dependencies"].append(Depends(RequiresScope(scope)))

            # Update responses with route_options responses or use latter if not set
            if "responses" in route_options:
                responses = {**(responses or {}), **route_options.pop("responses")}

            router.add_api_route(
                path,
                endpoint,
                tags=[self.name],
                operation_id=endpoint.__name__,
                name=name,
                responses=responses,
                **route_options,
            )
        return router


def clean_resources_same_name(resources: List[Resource]) -> List[Resource]:
    dct = {x.version: x for x in resources}
    if len(dct) != len(resources):
        raise RuntimeError(
            f"Resource with name {resources[0].name} "
            f"is defined multiple times with the same version."
        )
    for stability in [Stability.STABLE, Stability.BETA]:
        tmp_resources = {k: v for (k, v) in dct.items() if k.stability is stability}
        for version, resource in tmp_resources.items():
            dct[version.decrease_stability()] = resource.get_less_stable(dct)
    return list(dct.values())


def clean_resources(resources: Sequence[Resource]) -> List[Resource]:
    """Ensure that resources are consistent:

    - ordered by name
    - (tag, version) combinations should be unique
    - for stable resources, beta & alpha are autocreated if needed
    - for beta resources, alpha is autocreated if needed
    """
    result = []
    names = {x.name for x in resources}
    for name in sorted(names):
        result.extend(
            clean_resources_same_name([x for x in resources if x.name == name])
        )
    return result
