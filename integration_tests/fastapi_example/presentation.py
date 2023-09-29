from http import HTTPStatus
from typing import Optional

from fastapi import Depends
from fastapi import Response

from clean_python import DoesNotExist
from clean_python import Page
from clean_python import ValueObject
from clean_python.fastapi import delete
from clean_python.fastapi import get
from clean_python.fastapi import patch
from clean_python.fastapi import post
from clean_python.fastapi import RequestQuery
from clean_python.fastapi import Resource
from clean_python.fastapi import v

from .application import ManageBook
from .domain import Author
from .domain import Book


class BookCreate(ValueObject):
    author: Author
    title: str


class BookUpdate(ValueObject):
    author: Optional[Author] = None
    title: Optional[str] = None


class V1Books(Resource, version=v(1), name="books"):
    def __init__(self):
        self.manager = ManageBook()

    @get("/books", response_model=Page[Book])
    async def list(self, q: RequestQuery = Depends()):
        return await self.manager.filter([], q.as_page_options())

    @post("/books", status_code=HTTPStatus.CREATED, response_model=Book)
    async def create(self, obj: BookCreate):
        return await self.manager.create(obj.model_dump())

    @get("/books/{id}", response_model=Book)
    async def retrieve(self, id: int):
        return await self.manager.retrieve(id)

    @patch("/books/{id}", response_model=Book)
    async def update(self, id: int, obj: BookUpdate):
        return await self.manager.update(id, obj.model_dump(exclude_unset=True))

    @delete("/books/{id}", status_code=HTTPStatus.NO_CONTENT, response_class=Response)
    async def destroy(self, id: int):
        if not await self.manager.destroy(id):
            raise DoesNotExist("object", id)

    @get("/text")
    async def text(self):
        return Response("foo", media_type="text/plain")
