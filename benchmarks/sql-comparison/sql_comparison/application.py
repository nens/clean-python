from clean_python import Id
from clean_python import Manage

from .domain import TestModel
from .domain import TestModelSyncRepository


class ManageTestModel(Manage[TestModel]):
    pass


class SyncManageTestModel:
    def __init__(self, repo: TestModelSyncRepository):
        self.repo = repo

    def retrieve(self, id: Id) -> TestModel:
        return self.repo.get(id)
