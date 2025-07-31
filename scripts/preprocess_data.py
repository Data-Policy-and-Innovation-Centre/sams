import sys
from hamilton import driver
from loguru import logger

from sams import config  
from sams.preprocessing import pipeline as interim_pipeline
from sams.preprocessing import hss_pipeline

def main(args):
    if len(args) < 2:
        sys.exit(1)

    pipeline_name = args[1].lower()
    build = False
    final_vars = []

    if len(args) >= 3:
        build = args[2] == "build"
        final_vars = args[3:]

    if pipeline_name == "interim":
        pipeline_module = interim_pipeline
        if not final_vars:
            final_vars = [
                "save_nongeocoded_iti_students",
                "save_nongeocoded_diploma_students",
                "save_interim_iti_institutes"
            ]
        inputs = dict(build=build, google_maps=True)

    elif pipeline_name == "hss":
        pipeline_module = hss_pipeline
        if not final_vars:
            final_vars = [
                "save_hss_enrollment",
                "save_flattened_options",
                "save_compartment_subjects",
                "save_first_choice_admitted"
            ]
        inputs = dict(build=build)

    else:
        print(f"Unknown pipeline: {pipeline_name}. Use 'interim' or 'hss'.")
        sys.exit(1)

    dr = driver.Builder().with_modules(pipeline_module).build()
    results = dr.execute(final_vars=final_vars, inputs=inputs)

    logger.info("Pipeline execution complete.")
    for key, value in results.items():
        logger.info(f"{key}: {type(value)} - {getattr(value, 'shape', 'N/A')}")

if __name__ == "__main__":
    main(sys.argv)
