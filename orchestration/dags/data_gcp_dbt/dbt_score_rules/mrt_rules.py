from dbt_score import Model, RuleViolation, Severity, model_filter, rule


@model_filter
def only_mart_models(model: Model) -> bool:
    """Only applies a rule to schema X."""
    return model.name.lower().startswith("mrt_")


@model_filter
def only_described_models(model: Model) -> bool:
    """Only applies a rule to schema X."""
    return model.description


@rule(
    model_filters={only_mart_models(), only_described_models()}, severity=Severity.HIGH
)
def mrt_models_have_description(model: Model) -> RuleViolation | None:
    """A model should have a description well formatted."""
    if not model.description:
        return RuleViolation(message="Model lacks a description.")
    if len(model.description) < 10:
        return RuleViolation(message="Model description is too short.")


@rule(
    model_filters={only_mart_models(), only_described_models()}, severity=Severity.HIGH
)
def mrt_columns_have_description(model: Model) -> RuleViolation | None:
    """All columns of a model should have a description."""
    if len(model.columns) == 0:
        return RuleViolation(message="Model has no columns descriptions.")

    valid_column_names = [
        column.name for column in model.columns if len(column.description) > 10
    ]

    if len(valid_column_names) != len(model.columns):
        max_length = 60
        message = f"Model has columns without descriptions ({len(model.columns) - len(valid_column_names)})."
        if len(message) > max_length:
            message = f"{message[:60]}…"
        return RuleViolation(message=message)


@rule(
    model_filters={only_mart_models(), only_described_models()},
    severity=Severity.MEDIUM,
)
def mrt_model_have_tests(
    model: Model, tested_percentage: float = 0.1
) -> RuleViolation | None:
    """All columns of a model should have a description."""
    tested_columns = [column.name for column in model.columns if len(column.tests) > 0]
    if len(tested_columns) - len(model.columns) < tested_percentage:
        max_length = 60
        message = f"At least {tested_percentage*100}% of columns should have a test."
        if len(message) > max_length:
            message = f"{message[:60]}…"
        return RuleViolation(message=message)
