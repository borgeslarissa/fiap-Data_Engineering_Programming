import logging

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.utils import AnalysisException

logger = logging.getLogger(__name__)


class DataHandler:
    """
    Classe responsável pela leitura e escrita de dados.

    Todos os schemas são definidos explicitamente — sem inferência de tipos —
    garantindo performance e previsibilidade.
    """

    # ------------------------------------------------------------------
    # Schemas explícitos
    # ------------------------------------------------------------------

    SCHEMA_PEDIDOS = StructType([
        StructField("ID_PEDIDO",      StringType(),    True),
        StructField("PRODUTO",        StringType(),    True),
        StructField("VALOR_UNITARIO", FloatType(),     True),
        StructField("QUANTIDADE",     LongType(),      True),
        StructField("DATA_CRIACAO",   TimestampType(), True),
        StructField("UF",             StringType(),    True),
        StructField("ID_CLIENTE",     LongType(),      True),
    ])

    SCHEMA_PAGAMENTOS = StructType([
        StructField("id_pedido",         StringType(),    True),
        StructField("forma_pagamento",   StringType(),    True),
        StructField("valor_pagamento",   FloatType(),     True),
        StructField("status",            BooleanType(),   True),
        StructField("data_processamento", TimestampType(), True),
        StructField("avaliacao_fraude", StructType([
            StructField("fraude", BooleanType(), True),
            StructField("score",  FloatType(),   True),
        ]), True),
    ])

    def __init__(self, spark: SparkSession):
        self._spark = spark

    # ------------------------------------------------------------------
    # Leitura
    # ------------------------------------------------------------------

    def ler_pedidos(
        self, path: str, compression: str, header: bool, sep: str
    ) -> DataFrame:
        """
        Lê os arquivos CSV de pedidos com schema explícito e modo FAILFAST.

        :param path: Caminho para os arquivos CSV comprimidos.
        :param compression: Tipo de compressão (ex.: 'gzip').
        :param header: True se os arquivos possuem cabeçalho.
        :param sep: Separador de colunas.
        :return: DataFrame de pedidos.
        :raises AnalysisException: Se o caminho for inválido ou o arquivo não existir.
        :raises Py4JJavaError: Em caso de erros críticos na JVM.
        """
        logger.info(f"Lendo pedidos de: {path}")
        try:
            df = (
                self._spark.read
                .option("compression", compression)
                .option("mode", "FAILFAST")
                .csv(path, header=header, schema=self.SCHEMA_PEDIDOS, sep=sep)
            )
            logger.info("Pedidos lidos com sucesso.")
            return df
        except AnalysisException as e:
            logger.error(f"Erro de analise ao ler pedidos: {e}")
            raise
        except Py4JJavaError as e:
            logger.critical(f"Erro critico da JVM ao ler pedidos: {e}")
            raise

    def ler_pagamentos(self, path: str, compression: str) -> DataFrame:
        """
        Lê os arquivos JSON de pagamentos com schema explícito.

        :param path: Caminho para os arquivos JSON comprimidos.
        :param compression: Tipo de compressão (ex.: 'gzip').
        :return: DataFrame de pagamentos.
        :raises AnalysisException: Se o caminho for inválido ou o arquivo não existir.
        :raises Py4JJavaError: Em caso de erros críticos na JVM.
        """
        logger.info(f"Lendo pagamentos de: {path}")
        try:
            df = (
                self._spark.read
                .option("compression", compression)
                .json(path, schema=self.SCHEMA_PAGAMENTOS)
            )
            logger.info("Pagamentos lidos com sucesso.")
            return df
        except AnalysisException as e:
            logger.error(f"Erro de analise ao ler pagamentos: {e}")
            raise
        except Py4JJavaError as e:
            logger.critical(f"Erro critico da JVM ao ler pagamentos: {e}")
            raise

    # ------------------------------------------------------------------
    # Escrita
    # ------------------------------------------------------------------

    def escrever_parquet(self, df: DataFrame, path: str) -> None:
        """
        Grava o DataFrame em formato Parquet no caminho especificado.

        :param df: DataFrame a ser gravado.
        :param path: Caminho de destino.
        :raises Exception: Qualquer erro ocorrido durante a escrita.
        """
        logger.info(f"Gravando resultado em Parquet: {path}")
        try:
            df.write.mode("overwrite").parquet(path)
            logger.info(f"Dados gravados com sucesso em: {path}")
        except Exception as e:
            logger.error(f"Erro ao gravar Parquet em '{path}': {e}")
            raise
