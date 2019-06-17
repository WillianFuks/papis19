isort:
	pip install -U isort
	isort -rc transform

isort-check:
	isort -ns __init__.py -rc -c -df transform

flake8:
	pip install -U flake8
	flake8 transform
