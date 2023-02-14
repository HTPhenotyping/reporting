# This Makefile is less a build system and more a means of making running
# some development tasks more convenient.

.PHONY: all build clean lint reformat

PY_FILES := daily.py
PY_WHEEL_BASENAME := htpheno_reporting

all: reformat lint build

#---------------------------------------------------------------------------

reformat:
	poetry run isort -q $(PY_FILES)
	poetry run black -q $(PY_FILES)

lint:
	-poetry run bandit -qr $(PY_FILES)
	-poetry run mypy $(PY_FILES)
	-poetry run pylint $(PY_FILES)

requirements.txt: poetry.lock
	poetry export > requirements.txt

#---------------------------------------------------------------------------

build:
	poetry build

clean:
	rm -rf .mypy_cache/
	rm -rf dist/$(PY_WHEEL_BASENAME)-*.tar.gz
	rm -rf dist/$(PY_WHEEL_BASENAME)-*.whl
