extras="${EXTRAS:-fastapi,auth,celery,fluentbit,sql,sql-sync,s3,s3-sync,api-client,amqp,nanoid,test}"
pins="${PINS:-psycopg2-binary}"

mkdir -p var
pip install -e . --no-deps
python clean_python/testing/extract_minimum_requirements.py clean_python[$extras] > var/constraints.txt
pip-compile --upgrade --extra=$extras -c var/constraints.txt -o var/requirements.txt --no-header --no-annotate --strip-extras
pip install -r var/requirements.txt $pins
