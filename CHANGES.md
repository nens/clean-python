# Changelog of clean-python


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
