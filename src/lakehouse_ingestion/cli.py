"""CLI do framework."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable, List

from .contract_bundle import governance_check, governance_preview, load_contract_bundle
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
                normalized = dict(item)
                normalized.pop("_metadata", None)
                build_plan_from_kwargs(**normalized)
                count += 1
            print(f"OK {path} ({count} contrato(s))")
        except Exception as exc:
            exit_code = 1
            print(f"ERRO {path}: {exc}", file=sys.stderr)
    return exit_code


def _validate_bundles(paths: List[Path]) -> int:
    exit_code = 0
    for path in paths:
        try:
            bundle = load_contract_bundle(path)
            print(f"OK {path} (bundle para {bundle.ingestion.target_table})")
        except Exception as exc:
            exit_code = 1
            print(f"ERRO {path}: {exc}", file=sys.stderr)
    return exit_code


def _preview_governance(paths: List[Path], indent: int) -> int:
    exit_code = 0
    for path in paths:
        try:
            preview = governance_preview(load_contract_bundle(path))
            print(json.dumps(preview, indent=indent, sort_keys=True, default=str))
        except Exception as exc:
            exit_code = 1
            print(f"ERRO {path}: {exc}", file=sys.stderr)
    return exit_code


def _check_governance(paths: List[Path], indent: int) -> int:
    exit_code = 0
    for path in paths:
        try:
            report = governance_check(load_contract_bundle(path))
            print(json.dumps(report, indent=indent, sort_keys=True, default=str))
            if report["status"] == "FAILED":
                exit_code = 1
        except Exception as exc:
            exit_code = 1
            print(f"ERRO {path}: {exc}", file=sys.stderr)
    return exit_code


def _apply_governance(paths: List[Path]) -> int:
    from .ingestion import apply_governance_bundle

    exit_code = 0
    for path in paths:
        try:
            result = apply_governance_bundle(str(path))
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
        except Exception as exc:
            exit_code = 1
            print(f"ERRO {path}: {exc}", file=sys.stderr)
    return exit_code


def _apply_annotations(paths: List[Path]) -> int:
    from .ingestion import apply_annotations_bundle

    exit_code = 0
    for path in paths:
        try:
            result = apply_annotations_bundle(str(path))
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
        except Exception as exc:
            exit_code = 1
            print(f"ERRO {path}: {exc}", file=sys.stderr)
    return exit_code


def _apply_access(paths: List[Path], *, force_revoke: bool = False) -> int:
    from .ingestion import apply_access_bundle

    exit_code = 0
    for path in paths:
        try:
            result = apply_access_bundle(str(path), force_revoke=force_revoke)
            print(json.dumps(result, indent=2, sort_keys=True, default=str))
        except Exception as exc:
            exit_code = 1
            print(f"ERRO {path}: {exc}", file=sys.stderr)
    return exit_code


def _validate_access(paths: List[Path], indent: int) -> int:
    from ._sql import full_table_name
    from .governance import access_drift_report, validate_governance_contract

    exit_code = 0
    for path in paths:
        try:
            bundle = load_contract_bundle(path)
            plan = bundle.ingestion
            target = full_table_name(plan.catalog, plan.layer, plan.target_table)
            validation = validate_governance_contract(target, None, plan.access)
            drift = access_drift_report(target, plan.access)
            status = "FAILED" if validation["status"] == "FAILED" or drift["status"] == "FAILED" else "SUCCESS"
            report = {
                "status": status,
                "target_table": target,
                "validation": validation,
                "access_drift": drift,
            }
            print(json.dumps(report, indent=indent, sort_keys=True, default=str))
            if status == "FAILED":
                exit_code = 1
        except Exception as exc:
            exit_code = 1
            print(f"ERRO {path}: {exc}", file=sys.stderr)
    return exit_code


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="lakehouse-ingest")
    sub = parser.add_subparsers(dest="command", required=True)

    validate_parser = sub.add_parser("validate", help="Valida contratos YAML/JSON sem executar Spark")
    validate_parser.add_argument("paths", nargs="+", type=Path)

    validate_bundle_parser = sub.add_parser(
        "validate-bundle",
        help="Valida contrato .ingestion e arquivos irmaos annotations/operations/access",
    )
    validate_bundle_parser.add_argument("paths", nargs="+", type=Path)

    governance_preview_parser = sub.add_parser(
        "governance-preview",
        help="Gera preview SQL de annotations/access e payload operacional",
    )
    governance_preview_parser.add_argument("paths", nargs="+", type=Path)
    governance_preview_parser.add_argument("--indent", type=int, default=2)

    governance_check_parser = sub.add_parser(
        "governance-check",
        help="Valida annotations/access contra schema real do target",
    )
    governance_check_parser.add_argument("paths", nargs="+", type=Path)
    governance_check_parser.add_argument("--indent", type=int, default=2)

    drift_check_parser = sub.add_parser(
        "drift-check",
        help="Alias de governance-check para checagem de drift/contrato",
    )
    drift_check_parser.add_argument("paths", nargs="+", type=Path)
    drift_check_parser.add_argument("--indent", type=int, default=2)

    governance_apply_parser = sub.add_parser(
        "governance-apply",
        help="Aplica operations/annotations sem executar ingestao",
    )
    governance_apply_parser.add_argument("paths", nargs="+", type=Path)

    annotations_apply_parser = sub.add_parser(
        "apply-annotations",
        help="Aplica apenas annotations sem executar ingestao nem access",
    )
    annotations_apply_parser.add_argument("paths", nargs="+", type=Path)

    access_validate_parser = sub.add_parser(
        "validate-access",
        help="Valida apenas access.yaml e drift de grants sem aplicar alteracoes",
    )
    access_validate_parser.add_argument("paths", nargs="+", type=Path)
    access_validate_parser.add_argument("--indent", type=int, default=2)

    access_apply_parser = sub.add_parser(
        "apply-access",
        help="Aplica apenas access.yaml sem executar ingestao nem annotations",
    )
    access_apply_parser.add_argument("paths", nargs="+", type=Path)
    access_apply_parser.add_argument(
        "--force-revoke",
        action="store_true",
        help="Permite executar REVOKE para grants nao declarados quando access.revoke_unmanaged=true",
    )

    schema_parser = sub.add_parser("schema", help="Imprime JSON Schema dos contratos")
    schema_parser.add_argument("--indent", type=int, default=2)

    args = parser.parse_args(argv)
    if args.command == "validate":
        return _validate(args.paths)
    if args.command == "validate-bundle":
        return _validate_bundles(args.paths)
    if args.command == "governance-preview":
        return _preview_governance(args.paths, args.indent)
    if args.command in {"governance-check", "drift-check"}:
        return _check_governance(args.paths, args.indent)
    if args.command == "governance-apply":
        return _apply_governance(args.paths)
    if args.command == "apply-annotations":
        return _apply_annotations(args.paths)
    if args.command == "validate-access":
        return _validate_access(args.paths, args.indent)
    if args.command == "apply-access":
        return _apply_access(args.paths, force_revoke=args.force_revoke)
    if args.command == "schema":
        print(json.dumps(yaml_schema(), indent=args.indent, sort_keys=True))
        return 0
    parser.error(f"Comando não suportado: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
