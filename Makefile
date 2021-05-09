bootstrap:
	pip3 install -r requirements.txt

lint:
	flake8 orion test

test:
	PYTHONPATH=. python3 -m unittest discover -s test -v

cover:
	PYTHONPATH=. coverage run --source=orion -m unittest discover -s test -v
	coverage report -m

serve:
	PYTHONPATH=. python3 orion/server.py

init-db:
	PYTHONPATH=. python3 orion/scripts/db_init.py

.PHONY: bootstrap lint test cover
