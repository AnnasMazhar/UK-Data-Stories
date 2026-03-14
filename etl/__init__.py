"""ETL package."""

from etl.transform import (
    calculate_quality_score,
    deduplicate,
    generate_id,
    load_to_duckdb,
    run_etl,
    transform_record,
    transform_data_gov_uk_record,
    transform_ons_record,
    transform_police_uk_record,
    transform_parliament_record,
    transform_ckan_gov_uk_record,
)

__all__ = [
    "calculate_quality_score",
    "deduplicate",
    "generate_id",
    "load_to_duckdb",
    "run_etl",
    "transform_record",
    "transform_data_gov_uk_record",
    "transform_ons_record",
    "transform_police_uk_record",
    "transform_parliament_record",
    "transform_ckan_gov_uk_record",
]
