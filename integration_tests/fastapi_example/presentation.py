import base64
import json
import time
from http import HTTPStatus
from typing import Optional

from fastapi import Depends
from fastapi import Form
from fastapi import Request
from fastapi import Response
from fastapi import UploadFile
from fastapi.responses import JSONResponse
from fastapi.security import HTTPBasic
from fastapi.security import HTTPBasicCredentials

from clean_python import DoesNotExist
from clean_python import Page
from clean_python import ValueObject
from clean_python.fastapi import delete
from clean_python.fastapi import get
from clean_python.fastapi import patch
from clean_python.fastapi import post
from clean_python.fastapi import put
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


basic = HTTPBasic()


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

    @post("/form", response_model=Author)
    async def form(self, name: str = Form()):
        return {"name": name}

    @post("/file")
    async def file(self, file: UploadFile):
        return {file.filename: (await file.read()).decode()}

    @put("/urlencode/{name}", response_model=Author)
    async def urlencode(self, name: str):
        return {"name": name}

    @post("/token")
    def token(
        self,
        request: Request,
        grant_type: str = Form(),
        scope: str = Form(),
        credentials: HTTPBasicCredentials = Depends(basic),
    ):
        """For testing client credentials grant"""
        if request.headers["Content-Type"] != "application/x-www-form-urlencoded":
            return Response(status_code=HTTPStatus.METHOD_NOT_ALLOWED)
        if grant_type != "client_credentials":
            return JSONResponse(
                {"error": "invalid_grant"}, status_code=HTTPStatus.BAD_REQUEST
            )
        if credentials.username != "testclient":
            return JSONResponse(
                {"error": "invalid_client"}, status_code=HTTPStatus.BAD_REQUEST
            )
        if credentials.password != "supersecret":
            return JSONResponse(
                {"error": "invalid_client"}, status_code=HTTPStatus.BAD_REQUEST
            )
        if scope != "all":
            return JSONResponse(
                {"error": "invalid_grant"}, status_code=HTTPStatus.BAD_REQUEST
            )
        claims = {"user": "foo", "exp": int(time.time()) + 3600}
        payload = base64.b64encode(json.dumps(claims).encode()).decode()
        return {
            "access_token": f"header.{payload}.signature",
            "token_type": "Bearer",
            "expires_in": 3600,
        }
