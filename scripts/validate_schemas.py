#!/usr/bin/env python3
"""Validate all schemas/examples/**/*.json against their corresponding schema files.

Exit 0 if all pass, 1 if any fail.
"""

import json
import sys
from pathlib import Path

from jsonschema import Draft202012Validator, SchemaError


REPO_ROOT = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = REPO_ROOT / "schemas"
EXAMPLES_DIR = SCHEMAS_DIR / "examples"

SCHEMA_FILES = {
    "task":          SCHEMAS_DIR / "task.schema.json",
    "agent":         SCHEMAS_DIR / "agent.schema.json",
    "model":         SCHEMAS_DIR / "model.schema.json",
    "gpu_state":     SCHEMAS_DIR / "gpu_state.schema.json",
    "execution_log": SCHEMAS_DIR / "execution_log.schema.json",
    "note":          SCHEMAS_DIR / "note.schema.json",
    "log_event":     SCHEMAS_DIR / "log_event.schema.json",
}


def load_json(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    passed = 0
    failed = 0

    # 1. Verify schema files themselves are syntactically valid
    print("── Schema file integrity ──────────────────────────────")
    for name, schema_path in SCHEMA_FILES.items():
        if not schema_path.exists():
            print(f"  MISSING  {schema_path.relative_to(REPO_ROOT)}")
            failed += 1
            continue
        try:
            schema = load_json(schema_path)
            Draft202012Validator.check_schema(schema)
            print(f"  OK       {schema_path.relative_to(REPO_ROOT)}")
            passed += 1
        except SchemaError as e:
            print(f"  INVALID  {schema_path.relative_to(REPO_ROOT)}: {e.message}")
            failed += 1

    # 2. Validate every example against its schema
    print("\n── Example validation ─────────────────────────────────")
    for name, schema_path in SCHEMA_FILES.items():
        if not schema_path.exists():
            continue
        schema = load_json(schema_path)
        validator = Draft202012Validator(schema)

        example_dir = EXAMPLES_DIR / name
        if not example_dir.exists():
            print(f"  WARN  no examples directory: {example_dir.relative_to(REPO_ROOT)}")
            continue

        example_paths = sorted(example_dir.glob("*.json"))
        if not example_paths:
            print(f"  WARN  no examples found in {example_dir.relative_to(REPO_ROOT)}")
            continue

        for ex_path in example_paths:
            example = load_json(ex_path)
            errors = list(validator.iter_errors(example))
            rel = ex_path.relative_to(REPO_ROOT)
            if errors:
                for err in errors:
                    print(f"  FAIL  {rel}: [{err.json_path}] {err.message}")
                failed += 1
            else:
                print(f"  OK    {rel}")
                passed += 1

    # 3. Summary
    total = passed + failed
    print(f"\n{'─' * 55}")
    print(f"Result: {passed}/{total} passed", end="")
    if failed:
        print(f", {failed} FAILED  ✗")
    else:
        print("  ✓")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
