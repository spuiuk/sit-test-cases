SELF = $(lastword $(MAKEFILE_LIST))
ROOT_DIR = $(realpath $(dir $(SELF)))

PYTHONPATH := .
TEST_INFO_FILE := test-info.yml
PATH := $(PATH):/usr/local/bin
export PYTHONPATH TEST_INFO_FILE PATH

define runtox
	@cd "$(ROOT_DIR)" && tox -e $1
endef

.PHONY: test
test:
	$(call runtox, "pytest")

.PHONY: sanity_test
sanity_test:
	$(call runtox, "sanity")

.PHONY: check-mypy
check-mypy:
	$(call runtox, "mypy")

.PHONY: check-flake8
check-flake8:
	$(call runtox, "flake8")

.PHONY: check-black
check-black:
	$(call runtox, "black")

