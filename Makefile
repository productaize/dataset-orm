pypi:
	rm -rf dist
	python setup.py bdist_wheel
	twine check dist/*
	twine upload dist/*
