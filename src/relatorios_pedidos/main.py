"""
main.py — Aggregation Root

Este é o único ponto da aplicação responsável por:
  1. Configurar o logging.
  2. Instanciar todas as dependências.
  3. Injetar as dependências em cada componente.
  4. Executar o pipeline.
  5. Garantir o encerramento da sessão Spark (bloco finally).
"""
import logging
import sys

from relatorios_pedidos.config.settings import Settings
from relatorios_pedidos.io_utils.data_handler import DataHandler
from relatorios_pedidos.pipeline.pipeline import Pipeline
from relatorios_pedidos.processing.transformations import Transformation
from relatorios_pedidos.session.spark_session_manager import SparkSessionManager


def configurar_logging() -> None:
    """
    Configura o logging raiz da aplicação.
    Todos os módulos propagam seus logs até este handler.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("relatorio_pedidos.log"),
        ],
    )


def main() -> None:
    """
    Ponto de entrada da aplicação.
    Aggregation Root: instancia e injeta todas as dependências.
    """
    logger = logging.getLogger(__name__)

    # --- Configurações ---
    settings = Settings()

    # --- Sessão Spark ---
    session_manager = SparkSessionManager(app_name=settings.app_name)

    try:
        spark = session_manager.get_session()

        # --- Instância das dependências (Injeção de Dependências) ---
        data_handler = DataHandler(spark=spark)
        transformer = Transformation()

        # --- Pipeline recebe dependências injetadas ---
        pipeline = Pipeline(
            settings=settings,
            data_handler=data_handler,
            transformer=transformer,
        )

        # --- Execução ---
        pipeline.executar()

    except Exception as e:
        logger.error(f"FALHA CRITICA NO PIPELINE: {e}", exc_info=True)
        sys.exit(1)

    finally:
        session_manager.stop()


if __name__ == "__main__":
    configurar_logging()
    main()
