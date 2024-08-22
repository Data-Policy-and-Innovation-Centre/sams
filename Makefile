# Makefile

# Name of the environment
ENV_NAME=skills

# Path to the environment.yaml file
ENV_FILE=environment.yml

# Command to create the environment
create_env:
	conda env create -f $(ENV_FILE)

# Command to update the environment
update_env:
	conda env update -f $(ENV_FILE) --prune

# Command to remove the environment
remove_env:
	conda env remove -n $(ENV_NAME)


.PHONY: create_env update_env remove_env 

