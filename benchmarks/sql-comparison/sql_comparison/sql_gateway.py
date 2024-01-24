from clean_python.sql import SQLGateway
from clean_python.sql import SyncSQLGateway

from .sql_model import test_model


class TestModelGateway(SQLGateway, table=test_model):
    pass


class TestModelSyncGateway(SyncSQLGateway, table=test_model):
    pass
