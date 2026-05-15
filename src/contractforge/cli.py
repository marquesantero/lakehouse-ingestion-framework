"""CLI do ContractForge."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Iterable, List

from .config import VALID_LAYERS, VALID_SCHEMA_POLICIES, VALID_WRITE_MODES
from .contract_bundle import governance_check, governance_preview, load_contract_bundle
from .contract_schema import yaml_schema
from .maintenance import apply_ctrl_retention
from .plan import build_plan_from_kwargs, target_full_table_name
from .presets import apply_preset, list_presets, preset_details
from .sources import diagnose_source_connectors, list_source_connector_details, source_connector_details
from .templates import contract_template_details, contract_template_files, get_contract_template, list_contract_templates


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


def _write_yaml(path: Path, payload: dict[str, Any], *, force: bool = False) -> None:
    if path.exists() and not force:
        raise FileExistsError(f"{path} ja existe; use --force para sobrescrever")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        import yaml  # type: ignore
    except Exception as exc:
        raise RuntimeError("Geracao de YAML requer PyYAML instalado") from exc
    path.write_text(yaml.safe_dump(payload, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _csv_list(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


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


def _validate(paths: List[Path], *, expand_presets: bool = False) -> int:
    exit_code = 0
    for path in paths:
        try:
            payload = _load_contract(path)
            count = 0
            for item in _iter_contracts(payload):
                normalized = dict(item)
                normalized.pop("_metadata", None)
                plan = build_plan_from_kwargs(**normalized)
                if expand_presets:
                    expanded = apply_preset(normalized)
                    print(json.dumps(expanded, indent=2, sort_keys=True, default=str))
                elif plan.applied_presets:
                    print(f"PRESETS {path}: {', '.join(plan.applied_presets)}")
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


_CONTRACT_FILE_SUFFIXES = {".yaml", ".yml", ".json"}
_SPLIT_CONTRACT_MARKERS = (".annotations.", ".operations.", ".access.")


def _is_structured_contract_file(path: Path) -> bool:
    return path.is_file() and path.suffix.lower() in _CONTRACT_FILE_SUFFIXES


def _is_split_ingestion_file(path: Path) -> bool:
    return _is_structured_contract_file(path) and ".ingestion." in path.name


def _is_contracts_path(path: Path) -> bool:
    return "contracts" in {part.lower() for part in path.parts}


def _is_standalone_contract_file(path: Path) -> bool:
    if not _is_structured_contract_file(path):
        return False
    if ".ingestion." in path.name or any(marker in path.name for marker in _SPLIT_CONTRACT_MARKERS):
        return False
    return _is_contracts_path(path)


def _discover_project_contracts(root: Path) -> list[tuple[str, Path]]:
    if root.is_file():
        if _is_split_ingestion_file(root):
            return [("bundle", root)]
        return [("contract", root)] if _is_structured_contract_file(root) else []
    candidates = []
    for path in sorted(root.rglob("*")):
        if _is_split_ingestion_file(path):
            candidates.append(("bundle", path))
        elif _is_standalone_contract_file(path):
            candidates.append(("contract", path))
    return candidates


def _validate_project(paths: List[Path], indent: int) -> int:
    items = []
    for root in paths:
        discovered = _discover_project_contracts(root)
        if not discovered:
            items.append({"path": str(root), "kind": "project", "status": "FAILED", "error": "nenhum contrato encontrado"})
            continue
        for kind, path in discovered:
            try:
                if kind == "bundle":
                    bundle = load_contract_bundle(path)
                    items.append(
                        {
                            "path": str(path),
                            "kind": kind,
                            "status": "SUCCESS",
                            "target_table": bundle.ingestion.target_table,
                            "layer": bundle.ingestion.layer,
                            "target_schema": bundle.ingestion.target_schema or bundle.ingestion.layer,
                            "mode": bundle.ingestion.mode,
                            "split_files": bundle.paths or {},
                        }
                    )
                    continue
                payload = _load_contract(path)
                count = 0
                targets = []
                for item in _iter_contracts(payload):
                    normalized = dict(item)
                    normalized.pop("_metadata", None)
                    plan = build_plan_from_kwargs(**normalized)
                    targets.append(
                        {
                            "target_table": plan.target_table,
                            "layer": plan.layer,
                            "target_schema": plan.target_schema or plan.layer,
                            "mode": plan.mode,
                        }
                    )
                    count += 1
                items.append({"path": str(path), "kind": kind, "status": "SUCCESS", "contracts": count, "targets": targets})
            except Exception as exc:
                items.append({"path": str(path), "kind": kind, "status": "FAILED", "error": str(exc)})
    failed = [item for item in items if item["status"] == "FAILED"]
    report = {
        "status": "FAILED" if failed else "SUCCESS",
        "total": len(items),
        "succeeded": len(items) - len(failed),
        "failed": len(failed),
        "items": items,
    }
    print(json.dumps(report, indent=indent, sort_keys=True, default=str))
    return 1 if failed else 0


def _init_output_path(path: Path, *, split: bool) -> Path:
    if split:
        name = path.name
        for marker in (".ingestion.yaml", ".ingestion.yml", ".ingestion.json"):
            if name.endswith(marker):
                return path.with_name(name[: -len(marker)])
        return path
    if path.suffix.lower() in _CONTRACT_FILE_SUFFIXES:
        return path
    return path.with_suffix(".ingestion.yaml")


def _target_schema_arg(args: argparse.Namespace) -> str:
    return args.target_schema or args.layer


def _target_block(catalog: str, schema: str, target_table: str) -> dict[str, str]:
    return {"catalog": catalog, "schema": schema, "table": target_table}


def _build_init_ingestion_contract(args: argparse.Namespace) -> dict[str, Any]:
    mode = args.mode
    merge_keys = _csv_list(args.merge_keys)
    hash_keys = _csv_list(args.hash_keys) or merge_keys
    watermark_columns = _csv_list(args.watermark_columns)
    if mode in {"scd1_upsert", "scd2_historical", "snapshot_soft_delete"} and not merge_keys:
        raise ValueError(f"--merge-keys e obrigatorio para mode={mode}")
    if mode == "scd1_hash_diff" and not hash_keys:
        raise ValueError("--hash-keys ou --merge-keys e obrigatorio para mode=scd1_hash_diff")
    contract: dict[str, Any] = {
        "source": args.source,
        "target_table": args.target_table,
        "catalog": args.catalog,
        "layer": args.layer,
        "mode": mode,
        "schema_policy": args.schema_policy,
        "ctrl_schema": args.ctrl_schema,
    }
    if args.target_schema:
        contract["target_schema"] = args.target_schema
    if args.preset:
        contract["preset"] = _csv_list(args.preset)
    if merge_keys:
        contract["merge_keys"] = merge_keys
    if hash_keys and mode == "scd1_hash_diff":
        contract["hash_keys"] = hash_keys
    if watermark_columns:
        contract["watermark_columns"] = watermark_columns
    not_null = merge_keys or hash_keys
    if not_null:
        contract["quality_rules"] = {"not_null": not_null}
    return contract


def _build_init_annotations_contract(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "target": _target_block(args.catalog, _target_schema_arg(args), args.target_table),
        "table": {
            "description": args.description or f"TODO: descrever {args.target_table}",
            "aliases": [],
            "tags": {"domain": args.domain or "TODO"},
        },
        "columns": {},
    }


def _build_init_operations_contract(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "target": _target_block(args.catalog, _target_schema_arg(args), args.target_table),
        "ownership": {
            "business_owner": args.owner or "TODO",
            "technical_owner": args.technical_owner or "data-platform",
            "support_group": args.support_group or "data-platform",
        },
        "operations": {
            "criticality": args.criticality,
            "expected_frequency": args.expected_frequency,
            "freshness_sla_minutes": args.freshness_sla_minutes,
            "alert_on_failure": True,
            "alert_on_quality_fail": True,
            "runbook_url": args.runbook_url or "TODO",
            "tags": {},
        },
    }


def _build_init_access_contract(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "target": _target_block(args.catalog, _target_schema_arg(args), args.target_table),
        "access_policy": {"mode": "validate_only", "on_drift": "warn", "revoke_unmanaged": False},
        "grants": [{"principal": args.access_principal or "data-engineers", "privileges": ["SELECT"]}],
    }


def _init_contract(args: argparse.Namespace) -> int:
    try:
        output = _init_output_path(args.output, split=args.split)
        written = []
        ingestion = _build_init_ingestion_contract(args)
        if args.split:
            files = {
                output.with_suffix(".ingestion.yaml"): ingestion,
                output.with_suffix(".annotations.yaml"): _build_init_annotations_contract(args),
                output.with_suffix(".operations.yaml"): _build_init_operations_contract(args),
                output.with_suffix(".access.yaml"): _build_init_access_contract(args),
            }
            for path, payload in files.items():
                _write_yaml(path, payload, force=args.force)
                written.append(str(path))
        else:
            _write_yaml(output, ingestion, force=args.force)
            written.append(str(output))
        print(json.dumps({"status": "SUCCESS", "written": written}, indent=args.indent, sort_keys=True))
        return 0
    except Exception as exc:
        print(f"ERRO init: {exc}", file=sys.stderr)
        return 1


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
    from .bundles import apply_governance_bundle

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
    from .bundles import apply_annotations_bundle

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
    from .bundles import apply_access_bundle

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
    from .governance import access_drift_report, validate_governance_contract

    exit_code = 0
    for path in paths:
        try:
            bundle = load_contract_bundle(path)
            plan = bundle.ingestion
            target = target_full_table_name(plan)
            validation = validate_governance_contract(target, None, plan.access)
            drift = access_drift_report(target, plan.access)
            drift_failed = (
                drift["status"] == "FAILED"
                or (drift["status"] == "DRIFTED" and plan.access is not None and plan.access.on_drift == "fail")
            )
            status = "FAILED" if validation["status"] == "FAILED" or drift_failed else "SUCCESS"
            if status == "SUCCESS" and drift["status"] == "DRIFTED":
                status = "WARNED"
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


def _presets_list(indent: int) -> int:
    details = [preset_details(name) for name in list_presets()]
    print(json.dumps(details, indent=indent, sort_keys=True, default=str))
    return 0


def _presets_show(names: List[str], indent: int) -> int:
    exit_code = 0
    for name in names:
        try:
            print(json.dumps(preset_details(name), indent=indent, sort_keys=True, default=str))
        except Exception as exc:
            exit_code = 1
            print(f"ERRO {name}: {exc}", file=sys.stderr)
    return exit_code


def _connectors_list(indent: int) -> int:
    print(json.dumps(list_source_connector_details(), indent=indent, sort_keys=True, default=str))
    return 0


def _connectors_show(names: List[str], indent: int) -> int:
    exit_code = 0
    for name in names:
        try:
            print(json.dumps(source_connector_details(name), indent=indent, sort_keys=True, default=str))
        except Exception as exc:
            exit_code = 1
            print(f"ERRO {name}: {exc}", file=sys.stderr)
    return exit_code


def _connectors_doctor(names: List[str], indent: int) -> int:
    try:
        diagnostics = diagnose_source_connectors(names or None)
        print(json.dumps(diagnostics, indent=indent, sort_keys=True, default=str))
        return 1 if any(item["status"] == "missing_python_package" for item in diagnostics) else 0
    except Exception as exc:
        print(f"ERRO connectors doctor: {exc}", file=sys.stderr)
        return 1


def _template_output_base(path: Path) -> Path:
    name = path.name
    for marker in (".ingestion.yaml", ".ingestion.yml", ".ingestion.json"):
        if name.endswith(marker):
            return path.with_name(name[: -len(marker)])
    return path


def _templates_list(indent: int) -> int:
    details = [contract_template_details(name) for name in list_contract_templates()]
    print(json.dumps(details, indent=indent, sort_keys=True, default=str))
    return 0


def _templates_show(name: str, indent: int, *, metadata_only: bool = False) -> int:
    try:
        payload = contract_template_details(name) if metadata_only else get_contract_template(name)
        print(json.dumps(payload, indent=indent, sort_keys=True, default=str))
        return 0
    except Exception as exc:
        print(f"ERRO template {name}: {exc}", file=sys.stderr)
        return 1


def _templates_write(args: argparse.Namespace) -> int:
    try:
        output = _template_output_base(args.output)
        files = contract_template_files(args.name)
        written = []
        for kind, payload in files.items():
            path = output.with_suffix(f".{kind}.yaml")
            _write_yaml(path, payload, force=args.force)
            written.append(str(path))
        print(
            json.dumps(
                {"status": "SUCCESS", "template": args.name, "written": written},
                indent=args.indent,
                sort_keys=True,
            )
        )
        return 0
    except Exception as exc:
        print(f"ERRO templates write: {exc}", file=sys.stderr)
        return 1


def _maintenance_ctrl_retention(args: argparse.Namespace) -> int:
    try:
        result = apply_ctrl_retention(
            args.catalog,
            args.ctrl_schema,
            retention_days=args.retention_days,
            vacuum=args.vacuum,
            vacuum_retention_hours=args.vacuum_retention_hours,
            dry_run=not args.apply,
            targets=args.targets or None,
        )
        print(json.dumps(result, indent=args.indent, sort_keys=True, default=str))
        return 0
    except Exception as exc:
        print(f"ERRO maintenance ctrl-retention: {exc}", file=sys.stderr)
        return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="contractforge")
    sub = parser.add_subparsers(dest="command", required=True)

    validate_parser = sub.add_parser("validate", help="Valida contratos YAML/JSON sem executar Spark")
    validate_parser.add_argument("paths", nargs="+", type=Path)
    validate_parser.add_argument(
        "--expand-presets",
        action="store_true",
        help="Imprime o contrato expandido após aplicar presets",
    )

    validate_bundle_parser = sub.add_parser(
        "validate-bundle",
        help="Valida contrato .ingestion e arquivos irmaos annotations/operations/access",
    )
    validate_bundle_parser.add_argument("paths", nargs="+", type=Path)

    validate_project_parser = sub.add_parser(
        "validate-project",
        help="Descobre e valida recursivamente contratos em uma pasta de projeto",
    )
    validate_project_parser.add_argument("paths", nargs="+", type=Path)
    validate_project_parser.add_argument("--indent", type=int, default=2)

    init_parser = sub.add_parser(
        "init",
        help="Gera um contrato inicial YAML para acelerar novos pipelines",
    )
    init_parser.add_argument("--output", required=True, type=Path)
    init_parser.add_argument("--source", required=True)
    init_parser.add_argument("--target-table", required=True)
    init_parser.add_argument("--catalog", default="main")
    init_parser.add_argument("--layer", default="bronze", choices=sorted(VALID_LAYERS))
    init_parser.add_argument(
        "--target-schema",
        help="Schema físico do target. Quando omitido, usa o valor de --layer.",
    )
    init_parser.add_argument("--mode", default="scd0_append", choices=sorted(VALID_WRITE_MODES))
    init_parser.add_argument("--schema-policy", default="additive_only", choices=sorted(VALID_SCHEMA_POLICIES))
    init_parser.add_argument("--ctrl-schema", default="ops")
    init_parser.add_argument("--merge-keys")
    init_parser.add_argument("--hash-keys")
    init_parser.add_argument("--watermark-columns")
    init_parser.add_argument("--preset")
    init_parser.add_argument("--description")
    init_parser.add_argument("--domain")
    init_parser.add_argument("--owner")
    init_parser.add_argument("--technical-owner")
    init_parser.add_argument("--support-group")
    init_parser.add_argument("--criticality", default="medium", choices=["low", "medium", "high", "critical"])
    init_parser.add_argument(
        "--expected-frequency",
        default="daily",
        choices=["hourly", "daily", "weekly", "monthly", "ad_hoc"],
    )
    init_parser.add_argument("--freshness-sla-minutes", type=int, default=1440)
    init_parser.add_argument("--runbook-url")
    init_parser.add_argument("--access-principal")
    init_parser.add_argument("--split", action="store_true", help="Gera ingestion, annotations, operations e access")
    init_parser.add_argument("--force", action="store_true", help="Sobrescreve arquivos existentes")
    init_parser.add_argument("--indent", type=int, default=2)

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

    presets_parser = sub.add_parser("presets", help="Lista ou detalha presets declarativos")
    presets_sub = presets_parser.add_subparsers(dest="preset_command", required=True)
    presets_list_parser = presets_sub.add_parser("list", help="Lista presets disponíveis")
    presets_list_parser.add_argument("--indent", type=int, default=2)
    presets_show_parser = presets_sub.add_parser("show", help="Mostra detalhes de um ou mais presets")
    presets_show_parser.add_argument("names", nargs="+")
    presets_show_parser.add_argument("--indent", type=int, default=2)

    connectors_parser = sub.add_parser("connectors", help="Lista ou detalha conectores de source")
    connectors_sub = connectors_parser.add_subparsers(dest="connector_command", required=True)
    connectors_list_parser = connectors_sub.add_parser("list", help="Lista conectores registrados")
    connectors_list_parser.add_argument("--indent", type=int, default=2)
    connectors_show_parser = connectors_sub.add_parser("show", help="Mostra detalhes de um ou mais conectores")
    connectors_show_parser.add_argument("names", nargs="+")
    connectors_show_parser.add_argument("--indent", type=int, default=2)
    connectors_doctor_parser = connectors_sub.add_parser(
        "doctor",
        help="Diagnostica requisitos estáticos de conectores sem abrir conexões",
    )
    connectors_doctor_parser.add_argument("names", nargs="*")
    connectors_doctor_parser.add_argument("--indent", type=int, default=2)

    templates_parser = sub.add_parser("templates", help="Lista, exibe ou grava templates de contratos")
    templates_sub = templates_parser.add_subparsers(dest="template_command", required=True)
    templates_list_parser = templates_sub.add_parser("list", help="Lista templates de contratos")
    templates_list_parser.add_argument("--indent", type=int, default=2)
    templates_show_parser = templates_sub.add_parser("show", help="Mostra um template de contrato")
    templates_show_parser.add_argument("name")
    templates_show_parser.add_argument("--metadata-only", action="store_true")
    templates_show_parser.add_argument("--indent", type=int, default=2)
    templates_write_parser = templates_sub.add_parser("write", help="Grava um template como bundle YAML split")
    templates_write_parser.add_argument("name")
    templates_write_parser.add_argument("--output", required=True, type=Path)
    templates_write_parser.add_argument("--force", action="store_true", help="Sobrescreve arquivos existentes")
    templates_write_parser.add_argument("--indent", type=int, default=2)

    maintenance_parser = sub.add_parser("maintenance", help="Operacoes de manutencao operacional")
    maintenance_sub = maintenance_parser.add_subparsers(dest="maintenance_command", required=True)
    retention_parser = maintenance_sub.add_parser(
        "ctrl-retention",
        help="Gera ou aplica limpeza de historico das ctrl tables",
    )
    retention_parser.add_argument("--catalog", default="main")
    retention_parser.add_argument("--ctrl-schema", default="ops")
    retention_parser.add_argument("--retention-days", required=True, type=int)
    retention_parser.add_argument(
        "--target",
        dest="targets",
        action="append",
        help="Ctrl table logica a limpar; pode ser usado multiplas vezes. Default: todas historicas.",
    )
    retention_parser.add_argument("--vacuum", action="store_true", help="Inclui VACUUM apos DELETE")
    retention_parser.add_argument("--vacuum-retention-hours", type=int, default=168)
    retention_parser.add_argument(
        "--apply",
        action="store_true",
        help="Executa os comandos. Sem esta flag, apenas imprime o plano.",
    )
    retention_parser.add_argument("--indent", type=int, default=2)

    args = parser.parse_args(argv)
    if args.command == "validate":
        return _validate(args.paths, expand_presets=args.expand_presets)
    if args.command == "validate-bundle":
        return _validate_bundles(args.paths)
    if args.command == "validate-project":
        return _validate_project(args.paths, args.indent)
    if args.command == "init":
        return _init_contract(args)
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
    if args.command == "presets":
        if args.preset_command == "list":
            return _presets_list(args.indent)
        if args.preset_command == "show":
            return _presets_show(args.names, args.indent)
    if args.command == "connectors":
        if args.connector_command == "list":
            return _connectors_list(args.indent)
        if args.connector_command == "show":
            return _connectors_show(args.names, args.indent)
        if args.connector_command == "doctor":
            return _connectors_doctor(args.names, args.indent)
    if args.command == "templates":
        if args.template_command == "list":
            return _templates_list(args.indent)
        if args.template_command == "show":
            return _templates_show(args.name, args.indent, metadata_only=args.metadata_only)
        if args.template_command == "write":
            return _templates_write(args)
    if args.command == "maintenance":
        if args.maintenance_command == "ctrl-retention":
            return _maintenance_ctrl_retention(args)
    parser.error(f"Comando não suportado: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
