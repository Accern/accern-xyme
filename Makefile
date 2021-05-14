help:
	@echo "The following make targets are available:"
	@echo "lint-all	run all lint steps"
	@echo "lint-comment	run linter check over regular comments"
	@echo "lint-docstring	run linter check over docstring"
	@echo "lint-flake8	run flake8 checker to detect missing trailing comma"
	@echo "lint-forgottonformat	ensures format strings are used"
	@echo "lint-pycodestyle	run linter check using pycodestyle standard"
	@echo "lint-pycodestyle-debug	run linter in debug mode"
	@echo "lint-pylint	run linter check using pylint standard"
	@echo "lint-requirements	run requirements check"
	@echo "lint-stringformat	run string format check"
	@echo "lint-type-check	run type check"
	@echo "git-check	run git check"
	@echo "version-sync	check package versions are same"
	@echo "publish	publish the library on pypi"
	@echo "publish-ts	publish the library on npm"

lint-comment:
	! find . \( -name '*.py' -o -name '*.pyi' \) -and -not -path './venv/*' \
	| xargs grep --color=always -nE \
	  '#.*(todo|xxx|fixme|n[oO][tT][eE]:|Note:|nopep8\s*$$)|.\"^s%'

lint-docstring:
	flake8 --verbose --select DAR --exclude venv --show-source ./

lint-flake8:
	flake8 --verbose --select C815 --exclude venv --show-source ./

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
	mypy packages/python --config-file mypy.ini

lint-all: \
	lint-comment \
	lint-docstring \
	lint-forgottenformat \
	lint-requirements \
	lint-stringformat \
	lint-pycodestyle \
	lint-pylint \
	lint-type-check \
	lint-flake8

VERSION=`echo "from packages.python.accern_xyme import __version__;import sys;out = sys.stdout;out.write(__version__);out.flush();" | python3 2>/dev/null`
TS_VERSION=`echo "import json;fin=open('package.json', 'r');version=json.loads(fin.read())['version'];fin.close();print(version)" | python3 2>/dev/null`
CUR_TAG=`git describe --abbrev=10 --tags HEAD`

git-check:
	@git diff --exit-code 2>&1 >/dev/null && git diff --cached --exit-code 2>&1 >/dev/null || (echo "working copy is not clean" && exit 1)
	@test -z `git ls-files --other --exclude-standard --directory` || (echo "there are untracked files" && exit 1)
	@test `git rev-parse --abbrev-ref HEAD` = "master" || (echo "not on master" && exit 1)

version-sync:
	@test "${VERSION}" = "$(TS_VERSION)" || (echo "version not matching. VERSION=$(VERSION), TS_VERSION=$(TS_VERSION)" && exit 1)

publish:
	make git-check
	make version-sync
	rm -r dist build accern_xyme.egg-info || echo "no files to delete"
	python3 -m pip install -U setuptools twine wheel
	python3 setup.py sdist bdist_wheel
	python3 -m twine upload dist/accern_xyme-$(VERSION)-py3-none-any.whl dist/accern_xyme-$(VERSION).tar.gz
	git tag "v$(VERSION)"
	git push origin "v$(VERSION)"
	@echo "succesfully deployed $(VERSION)"

publish-ts:
	make git-check
	@test "${CUR_TAG}" = "v${VERSION}" || (echo "local tag $(CUR_TAG) != v$(VERSION)" && exit 1)
	yarn publish --new-version $(VERSION)
	@echo "succesfully deployed $(VERSION)"

type-information-python:
	cd packages/python/accern_xyme && \
		stubgen accern_xyme.py -o .

type-information-ts:
	cd packages/typescript && \
		tsc --p tsconfig-info.json
