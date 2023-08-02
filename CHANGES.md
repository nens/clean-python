# Changelog of clean-python


0.2.0b3 (2023-08-02)
--------------------

- Bugfix: remove bearer prefix from api-key


0.2.0b2 (2023-08-02)
--------------------

- ApiKey authentication now uses `Authorization` header and `bearer` prefix by default.


0.1.2 (2023-07-31)
------------------

- Added py.typed marker.


0.2.0b1 (2023-07-25)
--------------------

- `BadRequest` needs `.errors()` method to be backwards compatible.

- Added support for API key authentication.


0.2.0b0 (2023-07-23)
--------------------

- Pydantic 2.x support. Drops Pydantic 1.x support, use 0.1.x for Pydantic 1.x.


0.1.1 (2023-07-31)
------------------

- Various import fixes.

- Avoid inject==5.* because of its incompatibility with VS Code (pylance / pyright).


0.1.0 (2023-07-12)
------------------

- Initial project structure created with cookiecutter and
  [cookiecutter-python-template](https://github.com/nens/cookiecutter-python-template).

- Ported base functions from internal raster-service project.
