SET PYTHONPATH=.
pypy -m cProfile tests\speedtest_json.py
python -m cProfile tests\speedtest_json.py

