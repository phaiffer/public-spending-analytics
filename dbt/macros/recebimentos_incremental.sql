{#
    Incremental builds of the Recebimentos models rebuild whole launch months
    (delete+insert on launch_month_key), so a month never mixes two file versions.

    The intermediate model rebuilds a month when its staged file changed (new
    SHA-256) or when the classification seeds changed, which rebuilds every month.
    The marts rebuild the months the intermediate rebuilt after them. A change to
    the model SQL itself needs `dbt build --full-refresh`.
#}

{% macro recebimentos_classification_fingerprint() %}
    (
        select md5(
            (
                select string_agg(beneficiary_id || ':' || treasury_flow_class, ',' order by beneficiary_id)
                from {{ ref('treasury_bank_codes') }}
            )
            || '|'
            || (
                select string_agg(beneficiary_id, ',' order by beneficiary_id)
                from {{ ref('treasury_management_units') }}
            )
        )
    )
{% endmacro %}


{% macro recebimentos_changed_staged_months(staged_relation) %}
    select launch_month_key
    from (
        select
            launch_month_key,
            string_agg(distinct source_file_sha256, ',' order by source_file_sha256) as file_versions,
            {{ recebimentos_classification_fingerprint() }} as classification_fingerprint
        from {{ staged_relation }}
        group by launch_month_key

        except

        select
            launch_month_key,
            string_agg(distinct source_file_sha256, ',' order by source_file_sha256),
            max(classification_fingerprint)
        from {{ this }}
        group by launch_month_key
    )
{% endmacro %}


{% macro recebimentos_rebuilt_intermediate_months(intermediate_relation) %}
    select launch_month_key
    from (
        select launch_month_key, max(built_at) as built_at
        from {{ intermediate_relation }}
        group by launch_month_key

        except

        select launch_month_key, max(built_at)
        from {{ this }}
        group by launch_month_key
    )
{% endmacro %}
