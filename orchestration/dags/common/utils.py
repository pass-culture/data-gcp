def depends_loop(jobs: dict, default_upstream_operator):
    default_downstream_operators = []
    has_downstream_dependencies = []
    for _, jobs_def in jobs.items():

        operator = jobs_def["operator"]
        dependencies = jobs_def["depends"]
        default_downstream_operators.append(operator)

        if len(dependencies) == 0:
            operator.set_upstream(default_upstream_operator)
        for d in dependencies:
            depend_job = jobs[d]["operator"]
            has_downstream_dependencies.append(depend_job)
            operator.set_upstream(depend_job)

    return [
        x for x in default_downstream_operators if x not in has_downstream_dependencies
    ]
