
 # Makefile
#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_NAME = skills
PYTHON_VERSION = 3.10
PYTHON_INTERPRETER = python
ENV_FILE = environment.yml
PROJECT_ROOT := $(shell pwd)
PACKAGE_PATH := $(PROJECT_ROOT)/sams
SCRIPTS_PATH := $(PROJECT_ROOT)/scripts
BUILD_SAMS_SCRIPT := $(SCRIPTS_PATH)/build_sams_db.py
export PYTHONPATH := $(PROJECT_ROOT):$(PYTHONPATH)

#################################################################################
# COMMANDS                                                                      #
#################################################################################

# Run pipelines
clean_data:
	$(PYTHON_INTERPRETER) $(SCRIPTS_PATH)/preprocess_data.py

# Delete all compiled Python files and interim datasets
reset:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	find . -type f -name '*.log' -exec rm -f {} +
	find . -type f -name '*.db' -exec rm -f {} +
	find . -type f -name '*.db-journal' -exec rm -f {} +
	rm -rf cache/

# Build raw sams database
sams_db:
	@echo "Running build_sams_db.py with PYTHONPATH=$(PYTHONPATH)"
	$(PYTHON_INTERPRETER) $(BUILD_SAMS_SCRIPT)

# Tests
tests:
	@echo "Running tests..."
	uv run python -m pytest tests/

test_extract:
	
	@echo "Running test on extraction routine..."
	uv run python -m pytest tests/test_extract.py

test_client:
	
	@echo "Running tests on API client..."

# Build reports
reports:

	@echo "Build reports..."
	Rscript $(SCRIPTS_PATH)/build_reports.R

	

.PHONY: clean_data sams_db tests test_extract test_client preprocess

