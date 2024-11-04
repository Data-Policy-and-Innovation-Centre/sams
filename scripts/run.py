from hamilton import driver
from sams import pipeline
import sys


def main(args):
    if len(args) < 2:
        build = False
    else:
        build = args[1]

    dr = driver.Builder().with_modules(pipeline).build()
    final_vars = ["save_geocodes","save_interim_iti_students","save_interim_diploma_students"]
    inputs = dict(build=build,google_maps=True)
    dr.execute(final_vars=final_vars, inputs=inputs)


if __name__ == "__main__":
    main(sys.argv)
