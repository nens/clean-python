import asyncio
import os

from pydantic import HttpUrl

from clean_python import ctx
from clean_python import Tenant
from clean_python import User


def test_default_context():
    assert str(ctx.path) == "file://" + os.getcwd()
    assert ctx.user.id == "ANONYMOUS"
    assert ctx.user.name == "anonymous"
    assert ctx.tenant is None


async def test_task_isolation():
    async def get_set(user):
        ctx.user = user
        await asyncio.sleep(0.01)
        assert ctx.user == user

    await asyncio.gather(*[get_set(User(id=str(i), name="piet")) for i in range(10)])
    assert ctx.user.id == "ANONYMOUS"


async def test_tenant():
    tenant = Tenant(id=2, name="foo")
    ctx.tenant = tenant
    assert ctx.tenant == tenant

    ctx.tenant = None
    assert ctx.tenant is None


async def test_path():
    url = HttpUrl("http://testserver/foo?a=b")
    ctx.path = url
    assert ctx.path == url
