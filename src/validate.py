import json
import logging
from jsonschema import validate, ValidationError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("validate")

def load_schema(path):
    with open(path, "r") as f:
        return json.load(f)

def validate_records(df, schema_path, source_name):
    schema = load_schema(schema_path)
    valid_rows = []
    flagged_rows = []

    records = df.to_dict(orient="records")

    for idx, record in enumerate(records):
        try:
            validate(instance=record, schema=schema)
            valid_rows.append(record)
        except ValidationError as e:
            flagged_rows.append({"row_index": idx, "error": str(e.message), "record": record})

    if flagged_rows:
        logger.warning(f"{source_name}: {len(flagged_rows)} malformed records flagged, not dropped")

    return valid_rows, flagged_rows