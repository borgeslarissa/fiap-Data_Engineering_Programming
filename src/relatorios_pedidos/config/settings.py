import logging

import yaml

logger = logging.getLogger(__name__)


class Settings:
    """
    Classe responsável por carregar e fornecer as configurações
    centralizadas da aplicação a partir de um arquivo YAML.
    """

    def __init__(self, config_path: str = "config/settings.yaml"):
        self._config_path = config_path
        self._config: dict = {}
        self._carregar()

    def _carregar(self) -> None:
        logger.info(f"Carregando configuracoes de: {self._config_path}")
        try:
            with open(self._config_path, "r") as f:
                self._config = yaml.safe_load(f)
            logger.info("Configuracoes carregadas com sucesso.")
        except FileNotFoundError as e:
            logger.error(f"Arquivo de configuracao nao encontrado: {self._config_path}")
            raise e

    @property
    def app_name(self) -> str:
        return self._config["spark"]["app_name"]

    @property
    def path_pedidos(self) -> str:
        return self._config["paths"]["pedidos"]

    @property
    def path_pagamentos(self) -> str:
        return self._config["paths"]["pagamentos"]

    @property
    def path_output(self) -> str:
        return self._config["paths"]["output"]

    @property
    def pedidos_csv_compression(self) -> str:
        return self._config["file_options"]["pedidos_csv"]["compression"]

    @property
    def pedidos_csv_header(self) -> bool:
        return self._config["file_options"]["pedidos_csv"]["header"]

    @property
    def pedidos_csv_sep(self) -> str:
        return self._config["file_options"]["pedidos_csv"]["sep"]

    @property
    def pagamentos_json_compression(self) -> str:
        return self._config["file_options"]["pagamentos_json"]["compression"]

    @property
    def filtro_ano(self) -> int:
        return self._config["filters"]["ano"]
