import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class Transformation:
    """
    Classe que encapsula toda a lógica de negócio do pipeline.

    Métodos puros: recebem DataFrames e retornam DataFrames,
    sem efeitos colaterais — facilitando testes unitários.
    """

    def calcular_valor_total(self, df: DataFrame) -> DataFrame:
        """
        Adiciona a coluna VALOR_TOTAL = VALOR_UNITARIO * QUANTIDADE.

        :param df: DataFrame de pedidos.
        :return: DataFrame com a coluna VALOR_TOTAL adicionada.
        """
        logger.info("Calculando VALOR_TOTAL = VALOR_UNITARIO * QUANTIDADE")
        try:
            resultado = df.withColumn(
                "VALOR_TOTAL",
                F.col("VALOR_UNITARIO") * F.col("QUANTIDADE")
            )
            logger.info("VALOR_TOTAL calculado com sucesso.")
            return resultado
        except Exception as e:
            logger.error(f"Erro ao calcular VALOR_TOTAL: {e}")
            raise

    def filtrar_pedidos_por_ano(self, df: DataFrame, ano: int) -> DataFrame:
        """
        Filtra os pedidos mantendo apenas os do ano informado.

        :param df: DataFrame de pedidos.
        :param ano: Ano de referência para o filtro (ex.: 2025).
        :return: DataFrame filtrado.
        """
        logger.info(f"Filtrando pedidos do ano {ano}")
        try:
            resultado = df.filter(F.year(F.col("DATA_CRIACAO")) == ano)
            logger.info(f"Filtro por ano {ano} aplicado com sucesso.")
            return resultado
        except Exception as e:
            logger.error(f"Erro ao filtrar pedidos por ano: {e}")
            raise

    def filtrar_pagamentos_recusados_legitimos(self, df: DataFrame) -> DataFrame:
        """
        Filtra pagamentos com status=false (recusado) E fraude=false (legítimo).

        :param df: DataFrame de pagamentos.
        :return: DataFrame filtrado.
        """
        logger.info("Filtrando pagamentos: status=false e avaliacao_fraude.fraude=false")
        try:
            resultado = df.filter(
                (F.col("status") == False) &
                (F.col("avaliacao_fraude.fraude") == False)
            )
            logger.info("Filtro de pagamentos aplicado com sucesso.")
            return resultado
        except Exception as e:
            logger.error(f"Erro ao filtrar pagamentos: {e}")
            raise

    def join_pedidos_pagamentos(
        self,
        pedidos_df: DataFrame,
        pagamentos_df: DataFrame,
    ) -> DataFrame:
        """
        Realiza o JOIN entre pedidos e pagamentos filtrados,
        selecionando apenas as colunas do relatório.

        Colunas do relatório:
          - id_pedido
          - uf
          - forma_pagamento
          - valor_total
          - data_criacao

        :param pedidos_df: DataFrame de pedidos (com VALOR_TOTAL calculado e ano filtrado).
        :param pagamentos_df: DataFrame de pagamentos já filtrados.
        :return: DataFrame resultante do JOIN com as colunas do relatório.
        """
        logger.info("Realizando JOIN entre pedidos e pagamentos")
        try:
            resultado = (
                pedidos_df.alias("ped")
                .join(
                    pagamentos_df.alias("pag"),
                    F.col("ped.ID_PEDIDO") == F.col("pag.id_pedido"),
                    "inner",
                )
                .select(
                    F.col("ped.ID_PEDIDO").alias("id_pedido"),
                    F.col("ped.UF").alias("uf"),
                    F.col("pag.forma_pagamento"),
                    F.col("ped.VALOR_TOTAL").alias("valor_total"),
                    F.col("ped.DATA_CRIACAO").alias("data_criacao"),
                )
            )
            logger.info("JOIN realizado com sucesso.")
            return resultado
        except Exception as e:
            logger.error(f"Erro ao realizar JOIN: {e}")
            raise

    def ordenar_relatorio(self, df: DataFrame) -> DataFrame:
        """
        Ordena o relatório por UF, forma_pagamento e data_criacao (ascendente).

        :param df: DataFrame do relatório.
        :return: DataFrame ordenado.
        """
        logger.info("Ordenando relatorio por uf, forma_pagamento e data_criacao")
        try:
            resultado = df.orderBy(
                F.col("uf").asc(),
                F.col("forma_pagamento").asc(),
                F.col("data_criacao").asc(),
            )
            logger.info("Ordenacao aplicada com sucesso.")
            return resultado
        except Exception as e:
            logger.error(f"Erro ao ordenar relatorio: {e}")
            raise
