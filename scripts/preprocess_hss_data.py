# sams/run_hss_pipeline.py
import sys
from hamilton import driver
from sams.preprocessing import hss_pipeline 

def main(args):
    if len(args) < 2:
        build = False
        final_vars = [
            "save_hss_enrollment",
            "save_flattened_options",
            "save_compartment_subjects",
        ]
    else:
        build = args[1] == "build"
        final_vars = args[2:]

    dr = driver.Builder().with_modules(hss_pipeline).build()
    inputs = dict(build=build)
    dr.execute(final_vars=final_vars, inputs=inputs)

if __name__ == "__main__":
    main(sys.argv)
