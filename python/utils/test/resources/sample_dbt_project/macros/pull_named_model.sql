{#
    Wraps ref() behind a macro argument, the same way CSCL's
    apply_text_formatting_from_seed(seed_name) hides a seed ref() inside a macro call
    rather than writing it literally in the calling model's SQL. A regex scan of the
    calling model's raw text can't see this dependency; a compiled manifest can.
#}
{% macro pull_named_model(model_name) %}
{{ ref(model_name) }}
{% endmacro %}
