"""Fixtures pytest com SparkSession + Delta Lake locais.

Testes que dependem de Spark devem usar a fixture ``spark``. Testes puros
(parsing de plan, validações) não precisam dela.
"""
from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _build_spark(warehouse: Path):
    if os.name == "nt":
        local_hadoop = ROOT / ".hadoop"
        if (local_hadoop / "bin" / "winutils.exe").exists() and (local_hadoop / "bin" / "hadoop.dll").exists():
            os.environ["HADOOP_HOME"] = str(local_hadoop)
            os.environ["PATH"] = str(local_hadoop / "bin") + os.pathsep + os.environ.get("PATH", "")
        spark_home = os.environ.get("SPARK_HOME")
        if spark_home and not (Path(spark_home) / "bin" / "spark-submit.cmd").exists():
            os.environ.pop("SPARK_HOME", None)
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ.setdefault("SPARK_LOCAL_HOSTNAME", "localhost")

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("contractforge-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
    )
    try:
        from delta import configure_spark_with_delta_pip  # type: ignore

        builder = configure_spark_with_delta_pip(builder)
    except Exception:
        pass
    return builder.getOrCreate()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """SparkSession com Delta configurado, escopo de sessão.

    Pula testes graciosamente se Java/Hadoop não estiverem disponíveis no host
    (ex.: máquinas dev sem JDK instalado).
    """
    if os.environ.get("SKIP_SPARK_TESTS") == "1":
        pytest.skip("SKIP_SPARK_TESTS=1")
    warehouse = tmp_path_factory.mktemp("warehouse")
    try:
        sess = _build_spark(warehouse)
    except (FileNotFoundError, OSError, RuntimeError) as exc:
        pytest.skip(f"Spark indisponível neste host: {exc}")
    sess.sparkContext.setLogLevel("ERROR")

    from contractforge import _spark as spark_module

    spark_module._cached_session = sess  # type: ignore[attr-defined]

    for db in ("bronze", "silver", "gold", "ops"):
        sess.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    yield sess
    sess.stop()


@pytest.fixture
def unique_name():
    """Sufixo único por teste para evitar colisão de nomes de tabela."""
    return "t_" + uuid.uuid4().hex[:8]


@pytest.fixture
def make_df(spark):
    """Helper que cria DataFrames a partir de linhas + string de schema."""

    def _factory(rows, schema):
        return spark.createDataFrame(rows, schema)

    return _factory
