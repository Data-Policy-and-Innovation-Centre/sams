from hamilton import driver
from sams.preprocessing import pipeline as preprocessing_pipeline
import sys


def main(args):
    if len(args) < 2:
        build = False
        final_vars = [
            "save_interim_iti_diploma_students",
            "save_interim_diploma_institutes",
            "save_interim_iti_students",
            "save_interim_iti_institutes"
        ]
    else:
        build = bool(args[1] == "True")
        final_vars = args[2:]

    dr = driver.Builder().with_modules(preprocessing_pipeline).build()
    
    inputs = dict(build=build, google_maps=True)
    dr.execute(final_vars=final_vars, inputs=inputs)


if __name__ == "__main__":
    main(sys.argv)
