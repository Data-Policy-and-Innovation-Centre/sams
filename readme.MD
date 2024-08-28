# SAMS

## Prerequisites

Before you can use SAMS, you need to have Conda installed on your system and available on your PATH.

## Setting up the Environment

To set up the environment, follow these steps:

1. Open a terminal and navigate to the root directory of the SAMS repository.
2. Run the following command to create the Conda environment:
```bash
make create_env
```
This will create a new Conda environment named `skills`.
3. Activate the environment by running:
```bash
conda activate skills
```

## Running the project
To run the project, follow these steps:
1. Build the dataset using:
```bash
make dataset
```
