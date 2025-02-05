extras="${EXTRAS:-fastapi,auth,celery,fluentbit,sql,sql-sync,s3,s3-sync,api-client,amqp,nanoid,test}"
pins="${PINS:-psycopg2-binary}"

mkdir -p var

# The versions in ``pyproject.toml`` are compiled using pip-tools
# (see https://github.com/jazzband/pip-tools). This to ensure that all dependencies are consistent with
# one another. These are output to ``var/requirements.txt``.
pip-compile --upgrade --extra=$extras -o var/requirements.txt --no-header --no-annotate --strip-extras

# Then, the environment is created from the ``var/requirements.txt``
pip install -r var/requirements.txt $pins
