from typing import Dict, List, Tuple, Union, Any
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def detect_schema_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str]) -> Dict[str, Union[Dict[str, str], str]]:
    new_columns = {k: v for k, v in actual_schema.items() if k not in expected_schema}
    removed_columns = {k: v for k, v in expected_schema.items() if k not in actual_schema}
    type_changes = {k: (actual_schema[k], expected_schema[k]) for k in expected_schema if actual_schema[k]!= expected_schema[k]}
    drift_severity = 'NONE'
    if new_columns:
        if any(actual_schema[col] not in ['string', 'boolean'] or 'NULLABLE' not in actual_schema[col] for col in new_columns):
            drift_severity = 'HIGH'
        else:
            drift_severity = 'LOW'
    if removed_columns:
        drift_severity = 'BREAKING'
    return {
        "new_columns": new_columns,
        "removed_columns": removed_columns,
        "type_changes": type_changes,
        "drift_severity": drift_severity
    }

def decide_action(drift_report: Dict[str, Union[Dict[str, str], str]]) -> Dict[str, Dict[str, Union[str, int]]]:
    decisions = {}
    for col_name, col_type in drift_report['new_columns'].items():
        if col_type =='string':
            decisions[col_name] = {'action': 'ADD_TO_SCHEMA','reason': 'New nullable string column', 'risk_level': 0}
        elif col_type == 'boolean':
            decisions[col_name] = {'action': 'ADD_TO_SCHEMA','reason': 'New nullable boolean column', 'risk_level': 0}
        elif col_type in ['float', 'double']:
            decisions[col_name] = {'action': 'FLAG_ANOMALY','reason': 'New numeric column', 'risk_level': 1}
    for col_name in drift_report['removed_columns']:
        decisions[col_name] = {'action': 'HALT','reason': 'Removed column', 'risk_level': 2}
    return decisions

def apply_schema_evolution(spark_df: DataFrame, decisions: Dict[str, Dict[str, Union[str, int]]], updated_schema: Dict[str, str]) -> Tuple[DataFrame, List[str]]:
    migration_notes = []
    for col_name, decision in decisions.items():
        if decision['action'] == 'DROP_SILENTLY':
            spark_df = spark_df.drop(col_name)
        elif decision['action'] == 'ADD_TO_SCHEMA':
            migration_notes.append(f"Added new column: {col_name} with type {updated_schema[col_name]}")
        elif decision['action'] == 'FLAG_ANOMALY':
            spark_df = spark_df.withColumn(f"{col_name}_anomaly", F.lit(True))
            migration_notes.append(f"Flagged anomaly for column: {col_name}")
    return spark_df, migration_notes

def handle_drift(expected_schema: Dict[str, str], actual_schema: Dict[str, str], spark_df: DataFrame = None) -> Dict[str, Union[Dict, List, str]]:
    drift_report = detect_schema_drift(expected_schema, actual_schema)
    decisions = decide_action(drift_report)
    print(f"Drift detected: {drift_report['drift_severity']}")
    if spark_df is not None:
        evolved_df, migration_notes = apply_schema_evolution(spark_df, decisions, actual_schema)
        return {
            "drift_report": drift_report,
            "decisions": decisions,
            "migration_notes": migration_notes,
            "evolved_df": evolved_df
        }
    return {
        "drift_report": drift_report,
        "decisions": decisions
    }
