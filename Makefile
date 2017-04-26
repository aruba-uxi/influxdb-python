.PHONY: clean flake upload

clean:
	find . -name "*.pyc" -delete
	rm -rf build dist wheels venv *.egg-info

flake:
	flake8 .

upload:
	python setup.py sdist upload -r pypicloud