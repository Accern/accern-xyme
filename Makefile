help:
	@echo "The following make targets are available:"
	@echo "lint-comment	run linter check over regular comments"
	@echo "lint-emptyinit	main inits must be empty"
	@echo "lint-flake8	run flake8 checker to detect missing trailing comma"
	@echo "lint-forgottonformat	ensures format strings are used"
	@echo "lint-pycodestyle	run linter check using pycodestyle standard"
	@echo "lint-pycodestyle-debug	run linter in debug mode"
	@echo "lint-pylint	run linter check using pylint standard"
	@echo "lint-requirements	run requirements check"
	@echo "lint-stringformat	run string format check"
	@echo "lint-type-check	run type check"
	@echo "publish	publish the library on pypi"

lint-comment:
	! find . \( -name '*.py' -o -name '*.pyi' \) -and -not -path './venv/*' \
	| xargs grep --color=always -nE \
	  '#.*(todo|xxx|fixme|n[oO][tT][eE]:|Note:|nopep8\s*$$)|.\"^s%'

lint-emptyinit:
	[ ! -s monitor/__init__.py ] && [ ! -s worker/__init__.py ]

lint-flake8:
	flake8 ./ --verbose --select C812 --exclude venv --show-source

lint-forgottenformat:
	! ./forgottenformat.sh

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

lint-requirements:
	sort -cf requirements.txt
	sort -cf requirements.lint.txt

lint-stringformat:
	! find . \( -name '*.py' -o -name '*.pyi' \) -and -not -path './venv/*' \
	| xargs grep --color=always -nE "%[^'\"]*\"\\s*%\\s*"

lint-type-check:
	mypy . --config-file mypy.ini

VERSION=`echo "import accern_xyme;print(accern_xyme.__version__)" | python 2>/dev/null`

publish:
	@git diff --exit-code 2>&1 >/dev/null && git diff --cached --exit-code 2>&1 >/dev/null || (echo "working copy is not clean" && exit 1)
	@test -z `git ls-files --other --exclude-standard --directory` || (echo "there are untracked files" && exit 1)
	@test `git rev-parse --abbrev-ref HEAD` = "master" || (echo "not on master" && exit 1)
	python setup.py sdist bdist_wheel
	twine upload dist/accern_xyme-$(VERSION)-py2.py3-none-any.whl dist/accern_xyme-$(VERSION).tar.gz
	git tag "v$(VERSION)"
	git push origin "v$(VERSION)"
	@echo "succesfully deployed $(VERSION)"
