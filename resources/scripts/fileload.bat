SET PYTHONPATH=.
CALL pypy .\bugzilla_etl\replicate.py --settings=fileload_settings.json
