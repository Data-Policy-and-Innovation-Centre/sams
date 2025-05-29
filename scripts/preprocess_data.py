from hamilton import driver
from sams.preprocessing import pipeline as interim_pipeline
import sys


def main(args):
    if len(args) < 2:
        build = False
        final_vars = [
            "save_nongeocoded_iti_students",
            "save_nongeocoded_diploma_students",
            "save_interim_iti_institutes"
        ]
    else:
        build = args[1] == "build"
        final_vars = args[2:]

    dr = driver.Builder().with_modules(interim_pipeline).build()
    
    inputs = dict(build=build, google_maps=True)
    dr.execute(final_vars=final_vars, inputs=inputs)


if __name__ == "__main__":
    main(sys.argv)
