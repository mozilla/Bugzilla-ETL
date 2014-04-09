SET PYTHONPATH=.
CALL pypy bzETL\replicate.py --settings=resources/json/replicate_bugs_settings.json
CALL pypy bzETL\replicate.py --settings=resources/json/replicate_comments_settings.json
