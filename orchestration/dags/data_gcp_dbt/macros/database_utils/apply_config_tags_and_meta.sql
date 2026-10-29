{% macro apply_config_tags_and_meta(
    config_params, base_tags=[], tags=None, base_meta={}, meta=None
) %}
    {#
        Shared boilerplate for custom_table_config, custom_incremental_config and
        custom_snapshot_config: merge each macro's own base tags/meta with the
        caller-supplied tags/meta, apply them onto config_params (only when
        non-empty), and return it. Keeps the merge logic in one place instead of
        duplicating it per macro.
    #}
    {% set merged_tags = (base_tags + (tags | default([], true))) | unique | list %}
    {% set merged_meta = {} %}
    {% do merged_meta.update(base_meta) %}
    {% do merged_meta.update(meta | default({}, true)) %}
    {% if merged_tags %}
        {% do config_params.update({"tags": merged_tags}) %}
    {% endif %}
    {% if merged_meta %}
        {% do config_params.update({"meta": merged_meta}) %}
    {% endif %}
    {{ return(config_params) }}
{% endmacro %}
