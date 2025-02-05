# Changelog of clean-python

## 0.18.1 (2025-02-05)
----------------------

- Test against PostGRES 16 (instead of 14).

- Test against Python 3.13 (bumped asyncpg to `0.30.*` and aiohttp to `3.10.*`).

- Fixed a bug in (Sync)ApiProvider and Gateway: if a query parameter with value `None`
  is presented, this now hides the query parameter from the generated url. Before,
  it resulted in `path?foo=None`.

- Removed version pins on the dependencies. Instead we now test each week against
  the latest versions, and automatically create an issue if there is something wrong.
  Also the minimum versions are tested.

- Allow boto3 version 1.36. Note that this has a known incompatibility with older versions
  of MinIO; upgrade minio to the latest versions to fix issues with missing Content-MD5 headers.
  See: https://github.com/minio/minio/issues/20845


## 0.18.0 (2024-10-21)
----------------------

- Bumped FastAPI to 0.115.* so that pydantic models are directly supported to
  declare query parameters. This deprecates the `RequestQuery.depends()` syntax,
  use `Annotated[RequestQuery, Query()]` instead.


## 0.17.2 (2024-10-10)
----------------------

- Fixed context forwarding to celery tasks in case Sentry is used.


## 0.17.1 (2024-10-01)
----------------------

- Add filename to generate s3 download url


## 0.17.0 (2024-09-23)
----------------------

- Added a `celery.CeleryConfig` with an `apply` method that properly sets up celery
  without making the tasks depending on the config. Also added integration tests that
  confirm the forwarding of context (tenant and correlation id).


## 0.16.5 (2024-09-12)
----------------------

- Reverted urllib3 to 2.0.* because of other constraints.


## 0.16.4 (2024-09-12)
----------------------

- Bumped dependencies.


## 0.16.3 (2024-09-11)
----------------------

- Allow to config/set `addressing_style` for S3 object storage. (default='virtual')

- Added `LRUCache` and `SyncLRUCache`.


## 0.16.2 (2024-07-16)
----------------------

- Removed psycopg2-binary dependency (because psycopg2 compilation is better
  for production envs).


## 0.16.1 (2024-07-15)
----------------------

- Reverted "FastAPI's favicon.ico with `/favicon.ico`" because of
  login issues in Swagger.


## 0.16.0 (2024-07-02)
----------------------

- Fixed datatype of ctx.path (it was starlette.URL, now it is pydantic_core.Url).

- Replaced FastAPI's favicon.ico with `/favicon.ico`.

- Deprecate `dramatiq` submodule.


## 0.15.2 (2024-05-29)
----------------------

- Use `psycopg2-binary` instead of `psycopg2`.


## 0.15.1 (2024-05-29)
----------------------

- Fixed `ImportError` because of absent `asyncpg` when using only `sql-sync` dependency.


## 0.15.0 (2024-05-29)
----------------------

- Added a base class for providers (`Provider` / `SyncProvider`) with empty
  definitions of `connect` and `disconnect` (co)routines.

- Re-use the `S3Client` in `S3Provider`. Awaiting `S3Provider.connect()` at
  startup is now necessary.

- Re-use the `ClientSession` in `ApiProvider`. Awaiting `ApiProvider.connect()`
  at startup is now necessary.

- Added `SyncInternalGateway`.

- Breaking change in `InternalGateway`: it now requires a `.mapper` attribute
  instead of `.to_internal` and `.to_external` methods.

- Adapted underscore to hyphen in "extra" requirements: `sql_sync -> sql-sync`,
  `api_client -> api-client`. See PEP685.


## 0.14.1 (2024-05-28)
----------------------

- Added synchronous S3 interface (installable through optional `s3_sync` dependency).


## 0.14.0 (2024-05-22)
----------------------

- Added YAML version of the openapi spec under my.domain/v1/openapi.yaml.

- Fixed an issue introduced in 0.12.4 with the securitySchemas in the openapi spec. Now there
  is always 1 securitySchema named "OAuth2.

- Replace the `auth` setting in the `fastapi.Service` with a different type, which encompasses
  the (original) token verifier settings, oauth2 settings, and a new scope_verifier callable.

- Scopes supplied per-endpoint are now documented in the OpenAPI schema.


## 0.13.1 (2024-05-01)
----------------------

- Allow self-signed certificates in CeleryRmqBroker.


## 0.13.0 (2024-04-24)
----------------------

- Fixed synchronous usage of DomainEvent.

- Breaking change: the allowed values in `RequestQuery.order_by` should now be
  specified using a literal type (for example `Literal["id", "-id"]`) instead of the
  `enum` keyword argument. When using the `enum` keyword argument, an exception will be
  raised. Upside of this is that the OpenAPI spec now correctly lists the options for `order_by`.

- Exceptions raised in validators of `RequestQuery` subclasses now result in a `BadRequest`
  instead of an internal server error. This requires change: instances of `Depends(SomeQuery)`
  must be replaced by `SomeQuery.depends()`.


## 0.12.7 (2024-04-23)
----------------------

- Added nanoid submodule.


## 0.12.6 (2024-04-18)
----------------------

- Add synchronous usage of DomainEvent.


## 0.12.5 (2024-04-18)
----------------------

- Added ComparisonFilter and corresponding extensions to the sql and fastapi
  modules. A ComparisonFilter does value comparisons like less than.


## 0.12.4 (2024-04-18)
----------------------

- Added the option of making API routes public in the fastapi Resource.
  This is done by moving the auth dependencies from app to route level.

- Fixed SQLAlchemy deprecation warning having to do with `and_`.

- Bumped fastapi to 0.110.*.

- Bumped pyjwt to 2.8.*.

## 0.12.3 (2024-03-20)
-------------------

- Change the type of Tenant.id and User.id to `Id`. This allows more types as id.


0.12.2 (2024-02-21)
-------------------

- Expanded `Id` to include `UUID`. If a RootEntity's id is annotated with `UUID` it
  will be autogenerated on `.create()`.


0.12.1 (2024-02-20)
-------------------

- Added `SyncManage` as a synchronous alternative to `Manage`.


0.12.0 (2024-02-19)
-------------------

- Fixed starlette deprecation warning about lifespans.

- Added `on_shutdown` as an optional parameter for the Fastapi Service.

- Moved SQL query building from SQLGateway to a separate SQLBuilder class.
  Applications that use the SQLGateway should review custom query building functionality.

- Moved SQL row <-> domain model mapping to SQLGateway.mapper. Applications
  overriding this mapping (dict_to_row, rows_to_dict) should adapt.

- Finished SyncSQLGateway. The functionality mirrors that of the SQLGateway, only
  it doesn't support transactional updates and nested related models.


0.11.2 (2024-01-31)
-------------------

- Replaced ProfyleMiddleWare by VizTracerMiddleware.


0.11.1 (2024-01-29)
-------------------

- Added default type casting to string for json fields in AsyncpgSQLDatabase.


0.11.0 (2024-01-29)
-------------------

- Replaced SQLProvider with SQLAlchemyAsyncSQLDatabase. SQLProvider still exists,
  but is the baseclass of several implemenations. Another implementation is
  AsyncpgSQLDatase; this imlementation removes overhead from SQL query execution
  and prevents the use of greenlets, but it also adds overhead because query
  compilation isn't cached.

- Added a 'has_related' subclass argument to SQLGateway, which should be used whenever
  a gateway contains nested (related) gateways.

- Added SyncSQLProvider and a (partial) implementation SQLAlchemySyncSQLDatabase.

- Don't do any access logging if the access_logger_gateway is not provided
  to clean_python.fastapi.Service.

- Pinned dependencies for better guarantees of clean-python tests.

- Removed Python 3.8 and 3.9 tests, added Python 3.12 tests.

- Added ProfyleMiddleware.

- Reverted to the old 'optimistic' concurrency control in Repository.update. This
  can be switched to 'pessimistic' by using a keyword argument.


0.10.0 (2024-01-18)
-------------------

- Changed the internals of SQLProvider: asyncpg is now used directly for
  connection pooling, transaction management, query execution and parameter
  binding. This removes overhead from SQL query execution and prevents the
  use of greenlets.

0.9.6 (2023-12-20)
------------------

- Fixed celery task_failure_log in case of a crashed worker.


0.9.5 (2023-12-18)
------------------

- SyncApiProvider: also retry when the Retry-After response header is missing.

- ApiProvider: (sync and async) retry on all methods except POST.

- ApiProvider: (sync and async) retry on 429, 500, 502, 503, 504.


0.9.4 (2023-12-07)
------------------

- Use a timeout for fetching jwks in TokenVerifier.

- Changed celery task logger args/kwargs back to argsrepr/kwargsrepr.


0.9.3 (2023-12-04)
------------------

- Sanitize error responses.

- Remove 501 error response on NotImplementedError.

- Solved aiohttp 'Unclosed client session' warning.


0.9.2 (2023-11-23)
------------------

- Revert changes done in 0.9.1 in CCTokenGateway.

- Added CCTokenGateway.fetch_headers()

- Added optional 'headers' parameter to ApiProvider.


0.9.1 (2023-11-23)
------------------

- Renamed 'fetch_token' parameter in api client to 'headers_factory' and
  made it optional.

- Added 'headers_factory' to upload/download functions.

- Allow 201 "CREATED" status code in upload_file.


0.9.0 (2023-11-22)
------------------

- Manage.update now automatically retries if a Conflict is raised.

- AlreadyExists is not a subclass of Conflict anymore.


0.8.4 (2023-11-15)
------------------

- Fixed verification of client credentials tokens.


## 0.8.3 (2023-11-09)
------------------

- Adapted RequestQuery.filters() to deal with list query parameters.


0.8.2 (2023-11-07)
------------------

- Skip health check access logs.

- Fix access logging of correlation id.

- Workaround celery issues with message headers: use the body (kwargs) instead.


0.8.1 (2023-11-06)
------------------

- Fixed celery BaseTask for retried tasks.


0.8.0 (2023-11-06)
------------------

- Renamed clean_python.celery to clean_python.amqp; clean_python.celery now contains
  actual Celery abstractions.


0.7.1 (2023-11-01)
------------------

- Automatically dump and restore correlation_id in dramatiq actors.

- Fixed logging of correlation_id in fastapi access logger.


## 0.7.0 (2023-11-01)
-------------------

- Add correlation_id to logging and accept X-Correlation-Id header in
  fastapi service.

- Add `SyncFluentbitGateway`.

- Log the nanosecond-precision "time" instead of the second-precision logtime
  in `[Sync]FluentbitGateway`.


0.6.9 (2023-10-11)
------------------

- Disable the default multipart encoding in `SyncApiProvider`.

- Added `file` parameter to `ApiProvider` to upload files (async is a TODO).


0.6.8 (2023-10-10)
------------------

- Add `trailing_slash` option to `ApiProvider`.


0.6.7 (2023-10-09)
------------------

- Adapt call signature of the `fetch_token` callable in `ApiProvider`.

- Add `clean_python.oauth.client_credentials`.


0.6.6 (2023-10-04)
------------------

- Fix blocking behaviour of `fetch_token` in `ApiProvider`.

- Fix missing `api_client.Response`.


0.6.5 (2023-10-04)
------------------

- Added async `ApiProvider` and `ApiGateway`.

- Added `request_raw` to `ApiProvider` for handling arbitrary responses.


0.6.4 (2023-10-03)
------------------

- Allow value objects for `Repository` subclasses.


0.6.3 (2023-10-02)
------------------

- Add `Mapper` type use it in `SyncApiGateway.mapper`.


0.6.2 (2023-10-02)
------------------

- Encode url paths in `SyncApiProvider`.


0.6.1 (2023-10-02)
------------------

- Added tests for `SyncApiGateway` and made it compatible with `urllib==1.*`.


## 0.6.0 (2023-09-28)
------------------

- Added `SyncGateway`, `SyncRepository`, and `InMemorySyncGateway`.

- Added optional `api_client` subpackage (based on `urllib3`).

- Added `fastapi_profiler` and renamed existing `profiler` to `dramatiq_profiler`.


0.5.1 (2023-09-25)
------------------

- Added `S3Gateway.remove_filtered`.

- Added `clean_python.s3.KeyMapper`.


0.5.0 (2023-09-12)
------------------

- Adapt `InternalGateway` so that it derives from `Gateway`.

- Renamed the old `InternalGateway` to `TypedInternalGateway`.

- Added `SQLDatabase.truncate_tables()`.


0.4.3 (2023-09-11)
------------------

- Added `InternalGateway.update`.


0.4.2 (2023-09-04)
------------------

- Support adding/changing `responses` via route_options.


## 0.4.1 (2023-08-31)
------------------

- Added optional bind parameters for `execute` in `SQLProvider`,
  `SQLDatabase` and `SQLTransaction`.


0.4.0 (2023-08-29)
------------------

- Don't use environment variables in setup_debugger.

- Add Id type (replaces int), it can also be a string.

- Added S3Gateway.

- Reinstate static type linter (mypy).


## 0.3.4 (2023-08-28)
---------------------

- Fixed linting errors.


## 0.3.3 (2023-08-28)
------------------

- fixed typo in SQL query for creating extensions.


0.3.2 (2023-08-28)
------------------

- Added `SQLDatabase.create_extension()`.


0.3.1 (2023-08-16)
------------------

- Added `TokenVerifier.force()` for testing purposes.


0.3.0 (2023-08-16)
------------------

- Add `scope` kwarg to http_method decorators (get, post, etc.)

- Moved the `Context` (`ctx`) to `clean_python.base` and changed its attributes to
  `path`, `user` and `tenant`.

- The `SQLGateway` can now be constructed with `multitenant=True` which makes it
  automatically filter the `tenant` column with the current `ctx.tenant`.


0.2.2 (2023-08-03)
------------------

- Expand ctx.claims with user details.


0.2.1 (2023-08-03)
------------------

- Add HTTP Bearer to OpenAPI security schema.

- Import debugpy at module level on setup_debugger import. Don't check for DEBUG
  environment variable when setting up.


0.2.0 (2023-08-03)
------------------

- Pydantic 2.x support. Drops Pydantic 1.x support, use 0.1.x for Pydantic 1.x.
  See https://docs.pydantic.dev/latest/migration/

- `BadRequest` is a subclass of `Exception` instead of `ValidationError` / `ValueError`.

- `oauth2.OAuth2Settings` is split into two new objects: `TokenVerifierSettings` and
  `OAuth2SPAClientSettings`. The associated call signature of `Service` was changed.


0.1.2 (2023-07-31)
------------------

- Added py.typed marker.


0.1.1 (2023-07-31)
------------------

- Various import fixes.

- Avoid inject==5.* because of its incompatibility with VS Code (pylance / pyright).


0.1.0 (2023-07-12)
------------------

- Initial project structure created with cookiecutter and
  [cookiecutter-python-template](https://github.com/nens/cookiecutter-python-template).

- Ported base functions from internal raster-service project.
