import logging

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)


class SparkSessionManager:
    """
    Classe responsável por criar e gerenciar a SparkSession da aplicação.
    """

    def __init__(self, app_name: str):
        self._app_name = app_name
        self._spark: SparkSession | None = None

    def get_session(self) -> SparkSession:
        """
        Retorna a SparkSession, criando-a caso ainda não exista.
        """
        if self._spark is None:
            logger.info(f"Criando SparkSession: app_name='{self._app_name}'")
            self._spark = (
                SparkSession.builder
                .appName(self._app_name)
                .master("local[*]")
                .getOrCreate()
            )
            self._spark.sparkContext.setLogLevel("WARN")
            logger.info("SparkSession criada com sucesso.")
        return self._spark

    def stop(self) -> None:
        """
        Encerra a SparkSession se estiver ativa.
        """
        if self._spark:
            self._spark.stop()
            self._spark = None
            logger.info("SparkSession encerrada.")
