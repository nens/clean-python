# Changelog of clean-python


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
