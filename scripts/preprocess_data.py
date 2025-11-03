import sys
from hamilton import driver
from sams.preprocessing import iti_diploma_pipeline, hss_pipeline, deg_pipeline

# Pipeline configurations
pipeline_configs = {
    "deg": {
        "module": deg_pipeline,
        "default_nodes":[
            "save_deg_enrollments",
            "save_deg_applications",
            "save_deg_marks",
        ],
        "default_inputs": {},
    },  
    "hss": {
        "module": hss_pipeline,
        "default_nodes": [
            "save_hss_enrollments",
            "save_hss_applications",
            "save_hss_marks",
        ],
        "default_inputs": {},
    },
    "iti_diploma": {
        "module": iti_diploma_pipeline,
        "default_nodes": [
            "save_nongeocoded_iti_students",
            "save_nongeocoded_diploma_students",
            "save_interim_iti_institutes",
            "save_interim_diploma_institutes",

        ],
        "default_inputs": {"google_maps": True},
    },
}


def run_pipeline(pipeline_name, build=False, override_nodes=None):
    pipeline_config = pipeline_configs[pipeline_name]
    inputs = {**pipeline_config["default_inputs"], "build": build}
    target_nodes = override_nodes or pipeline_config["default_nodes"]

    print(f"\nRunning pipeline: {pipeline_name.upper()}")
    pipeline_driver = driver.Builder().with_modules(pipeline_config["module"]).build()
    results = pipeline_driver.execute(final_vars=target_nodes, inputs=inputs)

    completed_nodes = [node for node in target_nodes if node in results]
    if completed_nodes:
        print(f"Completed: {', '.join(completed_nodes)}")

    print(f"Finished pipeline: {pipeline_name.upper()}\n")


def main(args):
    build_mode = "build" in args

    # Example usage: python scripts/preprocess_data.py deg save_deg_applications
    if len(args) >= 3:
        pipeline_name = args[1]
        node_name = args[2]
        run_pipeline(pipeline_name, build=build_mode, override_nodes=[node_name])
    elif len(args) >= 2:
        pipeline_name = args[1]
        run_pipeline(pipeline_name, build=build_mode)
    else:
        # Default: run all pipelines
        for pipeline_name in pipeline_configs:
            run_pipeline(pipeline_name, build=build_mode)


if __name__ == "__main__":
    main(sys.argv)
