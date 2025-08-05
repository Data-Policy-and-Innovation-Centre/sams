import sys
from hamilton import driver
from sams.preprocessing import iti_diploma_pipeline as interim_pipeline
from sams.preprocessing import hss_pipeline

pipeline_configs = {
    "hss": {
        "module": hss_pipeline,
        "default_vars": [
            "save_hss_enrollments",
            "save_hss_applications",
            "save_hss_marks",
            "save_hss_first_choice_admissions"
        ],
        "inputs": {}
    },
    "interim": {
        "module": interim_pipeline,
        "default_vars": [
            "save_nongeocoded_iti_students",
            "save_nongeocoded_diploma_students",
            "save_interim_iti_institutes"
        ],
        "inputs": {
            "google_maps": True
        }
    }
}


def run_pipeline(name, build=False, final_vars=None):
    config = pipeline_configs[name]
    module = config["module"]
    inputs = config["inputs"].copy()
    inputs["build"] = build
    final_vars = final_vars or config["default_vars"]

    dr = driver.Builder().with_modules(module).build()
    dr.execute(final_vars=final_vars, inputs=inputs)

def main(args):
    if len(args) < 2:
        pipelines_to_run = list(pipeline_configs.keys())
        build = False
        custom_final_vars = {}
    else:
        arg = args[1].lower()
        if arg in pipeline_configs:
            pipelines_to_run = [arg]
        elif arg == "all":
            pipelines_to_run = list(pipeline_configs.keys())
        else:
            return  # Unknown pipeline, silently skip

        build = "build" in args[2:]
        final_vars = [a for a in args[2:] if a != "build"]
        custom_final_vars = {pipelines_to_run[0]: final_vars} if final_vars else {}

    for pipeline in pipelines_to_run:
        run_pipeline(
            pipeline,
            build=build,
            final_vars=custom_final_vars.get(pipeline)
        )

if __name__ == "__main__":
    main(sys.argv)

