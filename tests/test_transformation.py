"""
Testes unitários para a classe Transformation.

Padrão AAA:
  - Arrange  → prepara os dados de entrada e o resultado esperado
  - Act      → executa o método sob teste
  - Assert   → valida o resultado obtido

Os testes utilizam DataFrames sintéticos — sem dependência de arquivos reais.
"""
import sys
import os
from datetime import datetime

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from relatorio_pedidos.processing.transformations import Transformation


# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """SparkSession local compartilhada por toda a sessão de testes."""
    session = (
        SparkSession.builder
        .appName("TestTransformations")
        .master("local[1]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def transformer():
    """Instância de Transformation para cada teste."""
    return Transformation()


# ------------------------------------------------------------------
# Teste 1 — calcular_valor_total
# ------------------------------------------------------------------

def test_calcular_valor_total(spark, transformer):
    """
    Garante que VALOR_TOTAL = VALOR_UNITARIO * QUANTIDADE
    e que a coluna é adicionada corretamente ao DataFrame.
    """
    # Arrange
    schema = StructType([
        StructField("ID_PEDIDO",      StringType(), True),
        StructField("VALOR_UNITARIO", FloatType(),  True),
        StructField("QUANTIDADE",     LongType(),   True),
    ])
    dados = [
        ("p-001", 100.0, 2),   # esperado: 200.0
        ("p-002",  50.0, 3),   # esperado: 150.0
        ("p-003", 200.0, 1),   # esperado: 200.0
    ]
    df_entrada = spark.createDataFrame(dados, schema)

    # Act
    df_resultado = transformer.calcular_valor_total(df_entrada)

    # Assert
    assert "VALOR_TOTAL" in df_resultado.columns
    assert df_resultado.count() == 3

    linhas = {r["ID_PEDIDO"]: r["VALOR_TOTAL"] for r in df_resultado.collect()}
    assert linhas["p-001"] == pytest.approx(200.0, abs=0.01)
    assert linhas["p-002"] == pytest.approx(150.0, abs=0.01)
    assert linhas["p-003"] == pytest.approx(200.0, abs=0.01)


# ------------------------------------------------------------------
# Teste 2 — filtrar_pedidos_por_ano
# ------------------------------------------------------------------

def test_filtrar_pedidos_por_ano(spark, transformer):
    """
    Garante que apenas registros do ano informado são retidos.
    """
    # Arrange
    schema = StructType([
        StructField("ID_PEDIDO",    StringType(),    True),
        StructField("DATA_CRIACAO", TimestampType(), True),
    ])
    dados = [
        ("p-2025-a", datetime(2025, 3, 15)),
        ("p-2024-a", datetime(2024, 6, 1)),
        ("p-2025-b", datetime(2025, 11, 20)),
        ("p-2023-a", datetime(2023, 1, 1)),
    ]
    df_entrada = spark.createDataFrame(dados, schema)

    # Act
    df_resultado = transformer.filtrar_pedidos_por_ano(df_entrada, ano=2025)

    # Assert
    assert df_resultado.count() == 2
    ids = {r["ID_PEDIDO"] for r in df_resultado.collect()}
    assert ids == {"p-2025-a", "p-2025-b"}


# ------------------------------------------------------------------
# Teste 3 — filtrar_pagamentos_recusados_legitimos
# ------------------------------------------------------------------

def test_filtrar_pagamentos_recusados_legitimos(spark, transformer):
    """
    Garante que apenas pagamentos com status=False E fraude=False são retidos.
    """
    # Arrange
    schema_fraude = StructType([StructField("fraude", BooleanType(), True)])
    schema = StructType([
        StructField("id_pedido",       StringType(),  True),
        StructField("status",          BooleanType(), True),
        StructField("avaliacao_fraude", schema_fraude, True),
    ])
    dados = [
        Row(id_pedido="p1", status=False, avaliacao_fraude=Row(fraude=False)),  # ✓ entra
        Row(id_pedido="p2", status=True,  avaliacao_fraude=Row(fraude=False)),  # ✗ aprovado
        Row(id_pedido="p3", status=False, avaliacao_fraude=Row(fraude=True)),   # ✗ fraude
        Row(id_pedido="p4", status=True,  avaliacao_fraude=Row(fraude=True)),   # ✗ ambos
        Row(id_pedido="p5", status=False, avaliacao_fraude=Row(fraude=False)),  # ✓ entra
    ]
    df_entrada = spark.createDataFrame(dados)

    # Act
    df_resultado = transformer.filtrar_pagamentos_recusados_legitimos(df_entrada)

    # Assert
    assert df_resultado.count() == 2
    ids = {r["id_pedido"] for r in df_resultado.collect()}
    assert ids == {"p1", "p5"}


# ------------------------------------------------------------------
# Teste 4 — ordenar_relatorio
# ------------------------------------------------------------------

def test_ordenar_relatorio(spark, transformer):
    """
    Garante que o relatório é ordenado por uf → forma_pagamento → data_criacao.
    """
    # Arrange
    schema = StructType([
        StructField("id_pedido",      StringType(),    True),
        StructField("uf",             StringType(),    True),
        StructField("forma_pagamento", StringType(),   True),
        StructField("valor_total",    FloatType(),     True),
        StructField("data_criacao",   TimestampType(), True),
    ])
    dados = [
        ("p3", "SP", "PIX",            300.0, datetime(2025, 5, 10)),
        ("p1", "MG", "CARTAO_CREDITO", 100.0, datetime(2025, 1, 1)),
        ("p4", "SP", "BOLETO",         400.0, datetime(2025, 3, 1)),
        ("p2", "MG", "PIX",            200.0, datetime(2025, 2, 28)),
    ]
    df_entrada = spark.createDataFrame(dados, schema)

    # Act
    df_resultado = transformer.ordenar_relatorio(df_entrada)

    # Assert
    # Ordem esperada: MG/CARTAO_CREDITO, MG/PIX, SP/BOLETO, SP/PIX
    linhas = df_resultado.collect()
    assert linhas[0]["uf"] == "MG" and linhas[0]["forma_pagamento"] == "CARTAO_CREDITO"
    assert linhas[1]["uf"] == "MG" and linhas[1]["forma_pagamento"] == "PIX"
    assert linhas[2]["uf"] == "SP" and linhas[2]["forma_pagamento"] == "BOLETO"
    assert linhas[3]["uf"] == "SP" and linhas[3]["forma_pagamento"] == "PIX"
