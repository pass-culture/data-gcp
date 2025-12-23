build_and_push_docker_image = SSHGCEOperator(
        task_id="build_and_push_docker_image",
        instance_name="{{ params.instance_name }}",
        base_dir=BASE_DIR,
        command="python build_and_push_docker_image.py "
        "--experiment-name {{ params.model_version }} "
        "--model-name {{ params.model_name }} "
        "--container-worker {{ params.container_worker }} "
        "--base-serving-container-path {{ params.artifact_registry_base_path }} ",
    )