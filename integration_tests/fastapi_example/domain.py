from clean_python import Repository
from clean_python import RootEntity
from clean_python import ValueObject


class Author(ValueObject):
    name: str


class Book(RootEntity):
    author: Author
    title: str


class BookRepository(Repository[Book]):
    pass
