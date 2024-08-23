# Makefile
#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_NAME = skills
PYTHON_VERSION = 3.10
PYTHON_INTERPRETER = python
ENV_FILE=environment.yml

#################################################################################
# COMMANDS                                                                      #
#################################################################################

# Command to create the environment
create_env:
	conda env create -f $(ENV_FILE)

# Command to update the environment
update_env:
	conda env update -f $(ENV_FILE) --prune

# Command to remove the environment
remove_env:
	conda env remove -n $(PROJECT_NAME)

## Delete all compiled Python files
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete


.PHONY: create_env update_env remove_env clean

