import sys
from hamilton import driver
from loguru import logger

from sams import config  # triggers folder creation logic
from sams.preprocessing import hss_pipeline
from sams.config import datasets

def main(args):
    build = False
    final_vars = [
        "save_hss_enrollment",
        "save_flattened_options",
        "save_compartment_subjects",
        "save_first_choice_admitted",
    ]

    if len(args) >= 2:
        build = args[1] == "build"
        if len(args) > 2:
            final_vars = args[2:]

    dr = driver.Builder().with_modules(hss_pipeline).build()
    results = dr.execute(final_vars=final_vars, inputs={"build": build})

    logger.info("Pipeline execution complete.")
    for key, value in results.items():
        logger.info(f"{key}: {type(value)} - {getattr(value, 'shape', 'N/A')}")

if __name__ == "__main__":
    main(sys.argv)
