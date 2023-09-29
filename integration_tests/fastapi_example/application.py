from typing import Optional

from clean_python import InMemoryGateway
from clean_python import Manage

from .domain import Book
from .domain import BookRepository


class ManageBook(Manage[Book]):
    def __init__(self, repo: Optional[BookRepository] = None):
        if repo is None:
            repo = BookRepository(InMemoryGateway([]))
        self.repo = repo
