help:
	@echo "The following make targets are available:"
	@echo "lint-comment	run linter check over regular comments"
	@echo "lint-emptyinit	main inits must be empty"
	@echo "lint-stringformat	run string format check"
	@echo "lint-requirements	run requirements check"
	@echo "lint-pycodestyle-debug	run linter in debug mode"
	@echo "lint-pycodestyle	run linter check using pycodestyle standard"
	@echo "lint-pylint	run linter check using pylint standard"
	@echo "lint-flake8  run flake8 checker to deteck missing trailing comma"
	@echo "lint-type-check	run type check"
	@echo "pytest	run all test with pytest"

lint-comment:
	! find . \( -name '*.py' -o -name '*.pyi' \) -and -not -path './venv/*' \
	| xargs grep --color=always -nE \
	  '#.*(todo|xxx|fixme|n[oO][tT][eE]:|Note:|nopep8\s*$$)|.\"^s%'

lint-emptyinit:
	[ ! -s monitor/__init__.py ] && [ ! -s worker/__init__.py ]

lint-stringformat:
	! find . \( -name '*.py' -o -name '*.pyi' \) -and -not -path './venv/*' \
	| xargs grep --color=always -nE "%[^'\"]*\"\\s*%\\s*"

lint-forgottenformat:
	! ./forgottenformat.sh

lint-requirements:
	sort -cf requirements.txt
	sort -cf requirements.lint.txt

lint-pycodestyle:
	pycodestyle --exclude=venv --show-source .

lint-pycodestyle-debug:
	pycodestyle --exclude=venv -v --show-source .

lint-pylint:
	find . \( -name '*.py' -o -name '*.pyi' \) -and -not -path './venv/*' \
	-and -not -path './stubs/*' \
	| sort
	find . \( -name '*.py' -o -name '*.pyi' \) -and -not -path './venv/*' \
	-and -not -path './stubs/*' \
	| sort | xargs pylint -j 6

lint-type-check:
	mypy . --config-file mypy.ini

lint-flake8:
	flake8 ./ --verbose --select C812 --exclude venv --show-source

pytest:
	export USER_FILEPATH=./userdata; \
	pytest -xvv $(FILE)
