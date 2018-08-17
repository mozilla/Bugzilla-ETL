SET PYTHONPATH=.
CALL pypy bugzilla_etl\replicate.py --settings=resources/settings/replicate_bugs_settings.json
CALL pypy bugzilla_etl\replicate.py --settings=resources/settings/replicate_comments_settings.json
