from clean_python import DoesNotExist
from clean_python import Id
from clean_python import Repository
from clean_python import RootEntity
from clean_python import SyncGateway


class TestModel(RootEntity):
    t: str
    f: float
    b: bool


class TestModelRepository(Repository[TestModel]):
    pass


class TestModelSyncRepository:
    entity = TestModel

    def __init__(self, gateway: SyncGateway):
        self.gateway = gateway

    def get(self, id: Id) -> TestModel:
        res = self.gateway.get(id)
        if res is None:
            raise DoesNotExist("object", id)
        else:
            return self.entity(**res)
