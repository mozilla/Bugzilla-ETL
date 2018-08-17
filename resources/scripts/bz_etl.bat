SET PYTHONPATH=.;vendor
pypy .\bugzilla_etl\bz_etl.py --settings=bz_etl_settings.json %1 %2 %3 %4
