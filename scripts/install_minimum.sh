extras="${EXTRAS:-fastapi,auth,celery,fluentbit,sql,sql-sync,s3,s3-sync,api-client,amqp,nanoid,blinker,test}"
pins="${PINS:-psycopg2-binary}"

mkdir -p var

# First, ``clean_python`` is installed without any dependency.
pip install -e . --no-deps

# Requirements are retieved from ``clean_python`` package metadata,
# and minimum version specifiers are changed to
# equality version specifiers (e.g. ``pydantic>=2.9`` becomes ``pydantic==2.9``). These are written in a
# `constraints.txt` file.
python clean_python/testing/extract_minimum_requirements.py clean_python[$extras] > var/constraints.txt

# Then the versions in ``pyproject.toml`` are compiled using pip-tools
# (see https://github.com/jazzband/pip-tools). This to ensure that all dependencies are consistent with
# one another. These are output to ``var/requirements.txt``.
pip-compile --upgrade --extra=$extras -c var/constraints.txt -o var/requirements.txt --no-header --no-annotate --strip-extras

# Finally, the environment is created from the ``var/requirements.txt``
pip install -r var/requirements.txt $pins
