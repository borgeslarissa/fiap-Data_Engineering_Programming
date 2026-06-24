import logging

from relatorios_pedidos.config.settings import Settings
from relatorios_pedidos.io_utils.data_handler import DataHandler
from relatorios_pedidos.processing.transformations import Transformation

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Classe responsável por orquestrar o pipeline de dados.

    Recebe todas as dependências via construtor (injeção de dependências):
      - Settings       → configurações
      - DataHandler    → I/O
      - Transformation → lógica de negócio

    Não instancia nenhuma dependência internamente.
    """

    def __init__(
        self,
        settings: Settings,
        data_handler: DataHandler,
        transformer: Transformation,
    ):
        self._settings = settings
        self._data_handler = data_handler
        self._transformer = transformer

    def executar(self) -> None:
        """
        Executa as etapas do pipeline na sequência:
        1. Carga dos pedidos (CSV)
        2. Carga dos pagamentos (JSON)
        3. Cálculo do valor total do pedido
        4. Filtro dos pedidos pelo ano configurado
        5. Filtro dos pagamentos recusados e legítimos
        6. JOIN pedidos × pagamentos
        7. Ordenação do relatório
        8. Exibição de amostra
        9. Gravação em Parquet
        """
        logger.info("=" * 60)
        logger.info("Pipeline iniciado.")
        logger.info("=" * 60)

        # --- 1. Carga ---
        pedidos_df = self._data_handler.ler_pedidos(
            path=self._settings.path_pedidos,
            compression=self._settings.pedidos_csv_compression,
            header=self._settings.pedidos_csv_header,
            sep=self._settings.pedidos_csv_sep,
        )
        pagamentos_df = self._data_handler.ler_pagamentos(
            path=self._settings.path_pagamentos,
            compression=self._settings.pagamentos_json_compression,
        )

        # --- 2. Transformações ---
        pedidos_df = self._transformer.calcular_valor_total(pedidos_df)
        pedidos_df = self._transformer.filtrar_pedidos_por_ano(
            pedidos_df, self._settings.filtro_ano
        )
        pagamentos_filtrados_df = (
            self._transformer.filtrar_pagamentos_recusados_legitimos(pagamentos_df)
        )

        # --- 3. JOIN e ordenação ---
        relatorio_df = self._transformer.join_pedidos_pagamentos(
            pedidos_df, pagamentos_filtrados_df
        )
        relatorio_df = self._transformer.ordenar_relatorio(relatorio_df)

        # --- 4. Saída ---
        total = relatorio_df.count()
        logger.info(f"Total de registros no relatorio: {total}")
        relatorio_df.show(20, truncate=False)

        self._data_handler.escrever_parquet(
            df=relatorio_df,
            path=self._settings.path_output,
        )

        logger.info("=" * 60)
        logger.info("Pipeline concluido com sucesso.")
        logger.info("=" * 60)
