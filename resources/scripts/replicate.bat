SET PYTHONPATH=.
CALL pypy bzETL\replicate.py --settings=resources/scripts/replicate_bugs_settings.json
CALL pypy bzETL\replicate.py --settings=resources/scripts/replicate_comments_settings.json
