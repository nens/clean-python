import random
import string
from datetime import datetime
from datetime import timezone

from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import create_engine
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import text

test_model = Table(
    "test_model",
    MetaData(),
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("t", Text, nullable=False),
    Column("f", Float, nullable=False),
    Column("b", Boolean, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
)


def rand_str(length: int) -> str:
    return "".join(random.choice(string.printable) for _ in range(length))


def rand_float(min: float, max: float) -> float:
    return random.random() * (max - min) + min


def rand_bool() -> bool:
    return random.random() > 0.5


def rand_datetime() -> datetime:
    return datetime.utcfromtimestamp(rand_float(min=0.0, max=1705798597.0)).replace(
        tzinfo=timezone.utc
    )


def create_and_fill_db(postgres_url, dbname, n=100):
    root_engine = create_engine(
        f"postgresql+psycopg2://{postgres_url}", isolation_level="AUTOCOMMIT"
    )
    with root_engine.connect() as connection:
        connection.execute(text(f"DROP DATABASE IF EXISTS {dbname}"))
        connection.execute(text(f"CREATE DATABASE {dbname}"))
    root_engine.dispose()
    engine = create_engine(
        f"postgresql+psycopg2://{postgres_url}/{dbname}", isolation_level="AUTOCOMMIT"
    )
    with engine.connect() as connection:
        test_model.metadata.drop_all(engine)
        test_model.metadata.create_all(engine)
        connection.execute(
            test_model.insert(),
            [
                {
                    "t": rand_str(16),
                    "f": rand_float(0, 1000),
                    "b": rand_bool(),
                    "created_at": rand_datetime(),
                    "updated_at": rand_datetime(),
                }
                for _ in range(n)
            ],
        )
    engine.dispose()
