SET PYTHONPATH=.
CALL pypy bzETL\replicate.py --settings=resources/settings/replicate_bugs_settings.json
CALL pypy bzETL\replicate.py --settings=resources/settings/replicate_comments_settings.json
