extras="${EXTRAS:-fastapi,auth,celery,fluentbit,sql,sql-sync,s3,s3-sync,api-client,amqp,nanoid,test}"
pins="${PINS:-psycopg2-binary}"

mkdir -p var
pip-compile --upgrade --extra=$extras -o var/requirements.txt --no-header --no-annotate --strip-extras
pip install -r var/requirements.txt $pins
