# Changelog of clean-python


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
