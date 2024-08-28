
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
SCRIPTS_PATH := $(PROJECT_ROOT)/scripts/
export PYTHONPATH := $(PROJECT_ROOT):$(PYTHONPATH)

#################################################################################
# COMMANDS                                                                      #
#################################################################################

# Command to create the environment
create_env:
	conda env create -f $(ENV_FILE)

	@echo ">>> conda env created. Activate with:\nconda activate $(PROJECT_NAME)"

# Command to update the environment
update_env:
	conda env update -f $(ENV_FILE) --prune

	@echo ">>> conda env created. Activate with:\nconda activate $(PROJECT_NAME)"

# Command to remove the environment
remove_env:
	conda env remove -n $(PROJECT_NAME)

# Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete
	find . -type f -name '*.log' -exec rm -f {} +
	find . -type f -name '*.db' -exec rm -f {} +
	find . -type f -name '*.db-journal' -exec rm -f {} +

# Build dataset
dataset:
	@echo "Running script.py with PYTHONPATH=$(PYTHONPATH)"
	$(PYTHON_INTERPRETER) $(SCRIPTS_PATH)build_dataset.py
	

.PHONY: create_env update_env remove_env clean dataset

