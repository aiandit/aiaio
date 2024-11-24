PYTHON ?= python

all:

dist src-pkg wheel-pkg:
	$(PYTHON) -m build .

clean:
	! test -d .git || git clean -xfd --exclude dist

allclean:
	git clean -xfd

uninstall:
	$(PYTHON) -m pip uninstall -y pyaio

install:
	$(PYTHON) -m pip install .

install-dist: wheel-pkg
	$(PYTHON) -m pip install -I $(lastword $(shell ls -lrt dist/*.whl))

update: install-dist
	$(PYTHON) -m pip install -I $(lastword $(shell ls -lrt dist/*.whl))

update: wheel-pkg install-dist

check test:
	pytest -v

.PHONY: dist src-pkg wheel-pkg clean check test
