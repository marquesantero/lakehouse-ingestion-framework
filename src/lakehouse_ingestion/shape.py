"""Transformações declarativas de estrutura para JSON/struct/array."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DataType, StringType, StructType

from ._sql import as_list

ARRAY_MODES = {"keep", "to_json", "size", "first", "explode", "explode_outer"}
CARDINALITY_CHANGING_MODES = {"explode", "explode_outer"}


@dataclass(frozen=True)
class ShapeFlattenConfig:
    """Configuração de flatten recursivo de structs."""

    enabled: bool = False
    separator: str = "_"
    include: List[str] = field(default_factory=list)
    exclude: List[str] = field(default_factory=list)
    max_depth: int = 10


@dataclass(frozen=True)
class ShapeArrayConfig:
    """Tratamento declarativo de uma coluna array."""

    path: str
    mode: str = "keep"
    alias: Optional[str] = None
    allow_cartesian: bool = False


@dataclass(frozen=True)
class ShapeColumnConfig:
    """Extração/alias declarativo de campo top-level ou aninhado."""

    path: str
    alias: str


@dataclass(frozen=True)
class ShapeJsonConfig:
    """Parse declarativo de uma coluna string contendo JSON válido."""

    column: str
    schema: str
    alias: Optional[str] = None
    drop_source: bool = False


@dataclass(frozen=True)
class ShapeConfig:
    """Contrato de transformação estrutural pré-quality/write."""

    parse_json: List[ShapeJsonConfig] = field(default_factory=list)
    flatten: ShapeFlattenConfig = field(default_factory=ShapeFlattenConfig)
    arrays: List[ShapeArrayConfig] = field(default_factory=list)
    columns: Dict[str, ShapeColumnConfig] = field(default_factory=dict)
    allow_cardinality_change_on_bronze: bool = False


def normalize_shape(value: Any) -> Optional[ShapeConfig]:
    """Normaliza ``shape`` declarado em YAML/Python para ``ShapeConfig``."""
    if value is None:
        return None
    if isinstance(value, ShapeConfig):
        return value
    raw = _require_mapping(value, "shape")
    unexpected = set(raw) - {"parse_json", "flatten", "arrays", "columns", "allow_cardinality_change_on_bronze"}
    if unexpected:
        raise ValueError(f"shape possui campos não reconhecidos: {sorted(unexpected)}")
    return ShapeConfig(
        parse_json=_normalize_parse_json(raw.get("parse_json")),
        flatten=_normalize_flatten(raw.get("flatten")),
        arrays=_normalize_arrays(raw.get("arrays")),
        columns=_normalize_columns(raw.get("columns")),
        allow_cardinality_change_on_bronze=bool(raw.get("allow_cardinality_change_on_bronze", False)),
    )


def apply_shape(df: DataFrame, shape: Optional[ShapeConfig], *, layer: str) -> DataFrame:
    """Aplica transformações estruturais declarativas ao DataFrame."""
    if shape is None:
        return df
    _validate_cardinality_policy(shape, layer)
    _validate_cartesian_arrays(shape)
    df = _apply_parse_json(df, shape.parse_json)
    df = _apply_arrays(df, shape.arrays)
    df = _apply_columns(df, shape.columns)
    if shape.flatten.enabled:
        df = _flatten_structs(df, shape.flatten)
    return df


def _normalize_parse_json(value: Any) -> List[ShapeJsonConfig]:
    if value is None:
        return []
    if isinstance(value, dict) or isinstance(value, str):
        raise ValueError("shape.parse_json deve ser uma lista")
    configs = []
    aliases = set()
    for idx, item in enumerate(value):
        raw = _require_mapping(item, f"shape.parse_json[{idx}]")
        unexpected = set(raw) - {"column", "schema", "alias", "drop_source"}
        if unexpected:
            raise ValueError(f"shape.parse_json[{idx}] possui campos não reconhecidos: {sorted(unexpected)}")
        column = _required_path(raw.get("column"), f"shape.parse_json[{idx}].column")
        schema = str(raw.get("schema") or "").strip()
        if not schema:
            raise ValueError(f"shape.parse_json[{idx}].schema não pode ser vazio")
        alias = _optional_alias(raw.get("alias"), f"shape.parse_json[{idx}].alias")
        output_column = alias or column
        if "." in output_column:
            raise ValueError(
                f"shape.parse_json[{idx}].alias é obrigatório quando column é path aninhado: {column}"
            )
        drop_source = bool(raw.get("drop_source", False))
        if drop_source and "." in column:
            raise ValueError(f"shape.parse_json[{idx}].drop_source não é suportado para path aninhado: {column}")
        if output_column in aliases:
            raise ValueError(f"shape.parse_json possui alias/coluna duplicado: {output_column}")
        aliases.add(output_column)
        configs.append(
            ShapeJsonConfig(
                column=column,
                schema=schema,
                alias=alias,
                drop_source=drop_source,
            )
        )
    return configs


def _normalize_flatten(value: Any) -> ShapeFlattenConfig:
    if value is None:
        return ShapeFlattenConfig()
    if isinstance(value, bool):
        return ShapeFlattenConfig(enabled=value)
    raw = _require_mapping(value, "shape.flatten")
    unexpected = set(raw) - {"enabled", "separator", "include", "exclude", "max_depth"}
    if unexpected:
        raise ValueError(f"shape.flatten possui campos não reconhecidos: {sorted(unexpected)}")
    separator = str(raw.get("separator", "_")).strip()
    if not separator:
        raise ValueError("shape.flatten.separator não pode ser vazio")
    max_depth = int(raw.get("max_depth", 10))
    if max_depth <= 0:
        raise ValueError("shape.flatten.max_depth deve ser inteiro positivo")
    return ShapeFlattenConfig(
        enabled=bool(raw.get("enabled", False)),
        separator=separator,
        include=as_list(raw.get("include")),
        exclude=as_list(raw.get("exclude")),
        max_depth=max_depth,
    )


def _normalize_arrays(value: Any) -> List[ShapeArrayConfig]:
    if value is None:
        return []
    if isinstance(value, dict) or isinstance(value, str):
        raise ValueError("shape.arrays deve ser uma lista")
    arrays = []
    for idx, item in enumerate(value):
        raw = _require_mapping(item, f"shape.arrays[{idx}]")
        unexpected = set(raw) - {"path", "mode", "alias", "allow_cartesian"}
        if unexpected:
            raise ValueError(f"shape.arrays[{idx}] possui campos não reconhecidos: {sorted(unexpected)}")
        path = _required_path(raw.get("path"), f"shape.arrays[{idx}].path")
        mode = str(raw.get("mode", "keep")).strip()
        if mode not in ARRAY_MODES:
            raise ValueError(f"shape.arrays[{idx}].mode={mode!r} não é suportado. Valores: {sorted(ARRAY_MODES)}")
        alias = _optional_alias(raw.get("alias"), f"shape.arrays[{idx}].alias")
        if mode in CARDINALITY_CHANGING_MODES and not alias:
            alias = _default_alias(path)
        arrays.append(
            ShapeArrayConfig(
                path=path,
                mode=mode,
                alias=alias,
                allow_cartesian=bool(raw.get("allow_cartesian", False)),
            )
        )
    return arrays


def _normalize_columns(value: Any) -> Dict[str, ShapeColumnConfig]:
    if value is None:
        return {}
    raw_columns = _require_mapping(value, "shape.columns")
    columns = {}
    aliases = set()
    for path, config in raw_columns.items():
        source_path = _required_path(path, "shape.columns.<path>")
        if isinstance(config, str):
            alias = _optional_alias(config, f"shape.columns.{source_path}.alias")
        else:
            raw = _require_mapping(config, f"shape.columns.{source_path}")
            unexpected = set(raw) - {"alias"}
            if unexpected:
                raise ValueError(f"shape.columns.{source_path} possui campos não reconhecidos: {sorted(unexpected)}")
            alias = _optional_alias(raw.get("alias"), f"shape.columns.{source_path}.alias")
        alias = alias or _default_alias(source_path)
        if alias in aliases:
            raise ValueError(f"shape.columns possui alias duplicado: {alias}")
        aliases.add(alias)
        columns[source_path] = ShapeColumnConfig(path=source_path, alias=alias)
    return columns


def _apply_parse_json(df: DataFrame, configs: List[ShapeJsonConfig]) -> DataFrame:
    aliases = set(df.columns)
    for config in configs:
        data_type = _data_type_at_path(df.schema, config.column)
        if data_type is None:
            raise ValueError(f"shape.parse_json referencia coluna inexistente: {config.column}")
        if not isinstance(data_type, StringType):
            raise ValueError(
                f"shape.parse_json.{config.column} deve ser string; tipo encontrado: {data_type.simpleString()}"
            )
        alias = config.alias or config.column
        if alias in aliases and alias != config.column:
            raise ValueError(f"shape.parse_json produziria colisão com coluna existente: {alias}")
        df = df.withColumn(alias, F.from_json(_path_col(config.column), config.schema))
        aliases.add(alias)
        if config.drop_source and alias != config.column and config.column in df.columns:
            df = df.drop(config.column)
            aliases.discard(config.column)
    return df


def _apply_arrays(df: DataFrame, arrays: List[ShapeArrayConfig]) -> DataFrame:
    pending = [array for array in arrays if array.mode != "keep"]
    while pending:
        progressed = False
        remaining = []
        for array in pending:
            data_type = _data_type_at_path(df.schema, array.path)
            if data_type is None:
                remaining.append(array)
                continue
            if not isinstance(data_type, ArrayType):
                raise ValueError(f"shape.arrays.{array.path} não é array; tipo encontrado: {data_type.simpleString()}")
            df = _apply_array(df, array)
            progressed = True
        if not progressed:
            missing = [array.path for array in remaining]
            raise ValueError(
                "shape.arrays contém paths não resolvidos. "
                "Para arrays aninhados, declare também o explode do array pai e use o alias gerado. "
                f"paths={missing}"
            )
        pending = remaining
    return df


def _apply_array(df: DataFrame, array: ShapeArrayConfig) -> DataFrame:
    alias = array.alias or _default_alias(array.path)
    col = _path_col(array.path)
    if array.mode == "to_json":
        return df.withColumn(alias, F.to_json(col))
    if array.mode == "size":
        return df.withColumn(alias, F.size(col))
    if array.mode == "first":
        return df.withColumn(alias, F.element_at(col, 1))
    if array.mode == "explode":
        return df.withColumn(alias, F.explode(col))
    if array.mode == "explode_outer":
        return df.withColumn(alias, F.explode_outer(col))
    return df


def _apply_columns(df: DataFrame, columns: Dict[str, ShapeColumnConfig]) -> DataFrame:
    aliases = set(df.columns)
    for column in columns.values():
        if column.alias in aliases and column.alias != column.path:
            raise ValueError(f"shape.columns produziria colisão com coluna existente: {column.alias}")
        if _data_type_at_path(df.schema, column.path) is None:
            raise ValueError(f"shape.columns referencia path inexistente: {column.path}")
        df = df.withColumn(column.alias, _path_col(column.path))
        aliases.add(column.alias)
    return df


def _flatten_structs(df: DataFrame, flatten: ShapeFlattenConfig) -> DataFrame:
    projections = []
    aliases = set()
    top_level_columns = set(df.columns)
    include = set(flatten.include)
    exclude = set(flatten.exclude)
    for schema_field in df.schema.fields:
        path = schema_field.name
        if include and path not in include:
            projections.append(_path_col(path).alias(path))
            aliases.add(path)
            continue
        if _is_excluded(path, exclude):
            projections.append(_path_col(path).alias(path))
            aliases.add(path)
            continue
        if isinstance(schema_field.dataType, StructType):
            for leaf_path, alias in _struct_leaf_paths(schema_field.dataType, path, flatten, depth=1):
                if _is_excluded(leaf_path, exclude):
                    continue
                if alias in top_level_columns:
                    continue
                if alias in aliases:
                    raise ValueError(f"shape.flatten produziria coluna duplicada: {alias}")
                projections.append(_path_col(leaf_path).alias(alias))
                aliases.add(alias)
        else:
            if path in aliases:
                raise ValueError(f"shape.flatten produziria coluna duplicada: {path}")
            projections.append(_path_col(path).alias(path))
            aliases.add(path)
    return df.select(*projections)


def _struct_leaf_paths(
    struct: StructType,
    prefix: str,
    flatten: ShapeFlattenConfig,
    *,
    depth: int,
) -> List[tuple[str, str]]:
    leaves = []
    for schema_field in struct.fields:
        path = f"{prefix}.{schema_field.name}"
        alias = path.replace(".", flatten.separator)
        if isinstance(schema_field.dataType, StructType) and depth < flatten.max_depth:
            leaves.extend(_struct_leaf_paths(schema_field.dataType, path, flatten, depth=depth + 1))
        else:
            leaves.append((path, alias))
    return leaves


def _validate_cardinality_policy(shape: ShapeConfig, layer: str) -> None:
    if layer != "bronze" or shape.allow_cardinality_change_on_bronze:
        return
    changing = [array.path for array in shape.arrays if array.mode in CARDINALITY_CHANGING_MODES]
    if changing:
        raise ValueError(
            "shape com explode/explode_outer muda cardinalidade e é bloqueado em bronze por padrão. "
            "Use shape.allow_cardinality_change_on_bronze=true apenas quando esta mudança for intencional. "
            f"arrays={changing}"
        )


def _validate_cartesian_arrays(shape: ShapeConfig) -> None:
    groups: dict[str, list[ShapeArrayConfig]] = {}
    for array in shape.arrays:
        if array.mode not in CARDINALITY_CHANGING_MODES:
            continue
        groups.setdefault(_parent_path(array.path), []).append(array)
    conflicts = {
        parent: [array.path for array in arrays if not array.allow_cartesian]
        for parent, arrays in groups.items()
        if len(arrays) > 1 and any(not array.allow_cartesian for array in arrays)
    }
    if conflicts:
        raise ValueError(
            "shape.arrays contém múltiplos explodes irmãos que podem gerar produto cartesiano. "
            "Use allow_cartesian=true no contrato se essa multiplicação for intencional. "
            f"conflicts={conflicts}"
        )


def _data_type_at_path(schema: StructType, path: str) -> Optional[DataType]:
    current: DataType = schema
    for part in path.split("."):
        if not isinstance(current, StructType):
            return None
        field = next((field for field in current.fields if field.name == part), None)
        if field is None:
            return None
        current = field.dataType
        if isinstance(current, ArrayType) and part != path.split(".")[-1]:
            return None
    return current


def _path_col(path: str):
    return F.col(".".join(f"`{part}`" for part in path.split(".")))


def _parent_path(path: str) -> str:
    return ".".join(path.split(".")[:-1])


def _default_alias(path: str) -> str:
    return path.replace(".", "_")


def _is_excluded(path: str, exclude: set[str]) -> bool:
    return path in exclude or any(path.startswith(f"{item}.") for item in exclude)


def _require_mapping(value: Any, field: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field} deve ser um objeto/dict")
    return value


def _required_path(value: Any, field: str) -> str:
    path = str(value or "").strip()
    if not path:
        raise ValueError(f"{field} não pode ser vazio")
    if "[]" in path:
        raise ValueError(f"{field} deve usar path sem [] no contrato shape; declare arrays por path e alias")
    return path


def _optional_alias(value: Any, field: str) -> Optional[str]:
    if value is None:
        return None
    alias = str(value).strip()
    if not alias:
        raise ValueError(f"{field} não pode ser vazio")
    if "." in alias:
        raise ValueError(f"{field} deve ser nome de coluna simples, sem ponto")
    return alias
