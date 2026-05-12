"""CLI do framework."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable, List

from .contract_schema import yaml_schema
from .plan import build_plan_from_kwargs


def _load_contract(path: Path) -> Any:
    suffix = path.suffix.lower()
    text = path.read_text(encoding="utf-8")
    if suffix == ".json":
        return json.loads(text)
    try:
        import yaml  # type: ignore
    except Exception as exc:
        raise RuntimeError("Validação de YAML requer PyYAML instalado") from exc
    return yaml.safe_load(text)


def _iter_contracts(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("plans"), list):
        for item in payload["plans"]:
            if not isinstance(item, dict):
                raise ValueError("plans deve conter apenas objetos")
            yield item
        return
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                raise ValueError("Lista de contratos deve conter apenas objetos")
            yield item
        return
    if isinstance(payload, dict):
        yield payload
        return
    raise ValueError("Contrato deve ser objeto, lista de objetos ou objeto com plans[]")


def _validate(paths: List[Path]) -> int:
    exit_code = 0
    for path in paths:
        try:
            payload = _load_contract(path)
            count = 0
            for item in _iter_contracts(payload):
                build_plan_from_kwargs(**item)
                count += 1
            print(f"OK {path} ({count} contrato(s))")
        except Exception as exc:
            exit_code = 1
            print(f"ERRO {path}: {exc}", file=sys.stderr)
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse-ingest")
    sub = parser.add_subparsers(dest="command", required=True)

    validate_parser = sub.add_parser("validate", help="Valida contratos YAML/JSON sem executar Spark")
    validate_parser.add_argument("paths", nargs="+", type=Path)

    schema_parser = sub.add_parser("schema", help="Imprime JSON Schema dos contratos")
    schema_parser.add_argument("--indent", type=int, default=2)

    args = parser.parse_args(argv)
    if args.command == "validate":
        return _validate(args.paths)
    if args.command == "schema":
        print(json.dumps(yaml_schema(), indent=args.indent, sort_keys=True))
        return 0
    parser.error(f"Comando não suportado: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
