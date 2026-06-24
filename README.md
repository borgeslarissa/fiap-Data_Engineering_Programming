# Relatório de Pedidos Recusados Legítimos

Projeto PySpark desenvolvido como trabalho final da disciplina **Data Engineering Programming** — FIAP.

## Objetivo

Gerar um relatório de pedidos de venda cujos pagamentos foram **recusados** (`status=false`) e classificados como **legítimos** na avaliação de fraude (`fraude=false`), referentes ao ano de **2025**.

### Colunas do relatório

| Coluna | Descrição |
|---|---|
| `id_pedido` | Identificador do pedido |
| `uf` | Estado onde o pedido foi feito |
| `forma_pagamento` | Forma de pagamento utilizada |
| `valor_total` | Valor total do pedido (unitário × quantidade) |
| `data_criacao` | Data de criação do pedido |

O relatório é ordenado por `uf`, `forma_pagamento` e `data_criacao`, e gravado em formato **Parquet**.

---

## Estrutura do projeto

```
fiap-Data_Engineering_Programming/
│
├── config/
│   └── settings.yaml          # Configurações centralizadas (paths, filtros, Spark)
│
├── src/
│   └── relatorios_pedidos/
│       ├── config/
│       │   └── settings.py    # Classe Settings — carrega o YAML
│       ├── session/
│       │   └── spark_session_manager.py  # Classe SparkSessionManager
│       ├── io_utils/
│       │   └── data_handler.py           # Classe DataHandler — leitura e escrita
│       ├── processing/
│       │   └── transformations.py        # Classe Transformation — lógica de negócio
│       ├── pipeline/
│       │   └── pipeline.py               # Classe Pipeline — orquestração
│       └── main.py                       # Aggregation Root — injeção de dependências
│
├── tests/
│   └── test_transformations.py  # Testes unitários com pytest
│
├── data/
│   ├── input/
│   │   ├── datasets-csv-pedidos/data/pedidos/     # Arquivos CSV de pedidos
│   │   └── dataset-json-pagamentos/data/pagamentos/  # Arquivos JSON de pagamentos
│   └── output/
│       └── relatorio_pedidos_recusados_legitimos/  # Saída em Parquet (gerada)
│
├── pyproject.toml
├── requirements.txt
├── MANIFEST.in
└── README.md
```

---

## Pré-requisitos

- Python 3.10+
- Java 11+ (necessário para o PySpark)
- pip

Verifique as versões:

```bash
python --version
java -version
```

---

## Instalação

### 1. Clone o repositório

```bash
git clone https://github.com/borgeslarissa/fiap-Data_Engineering_Programming.git
cd fiap-Data_Engineering_Programming
```

### 2. Crie e ative um ambiente virtual

```bash
python -m venv .venv

# Linux/macOS
source .venv/bin/activate

# Windows
.venv\Scripts\activate
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

Ou instale o projeto como pacote (modo editável):

```bash
pip install -e ".[dev]"
```

---

## Datasets

Os datasets precisam ser baixados manualmente e colocados nos caminhos esperados.

### Pedidos (CSV)

Repositório: https://github.com/infobarbosa/datasets-csv-pedidos

Coloque os arquivos em:
```
data/input/datasets-csv-pedidos/data/pedidos/
```

### Pagamentos (JSON)

Repositório: https://github.com/infobarbosa/dataset-json-pagamentos

Coloque os arquivos em:
```
data/input/dataset-json-pagamentos/data/pagamentos/
```

---

## Configuração

Todas as configurações ficam em `config/settings.yaml`:

```yaml
spark:
  app_name: "Relatorio Pedidos Recusados Legitimos"

paths:
  pedidos: "./data/input/datasets-csv-pedidos/data/pedidos/"
  pagamentos: "./data/input/dataset-json-pagamentos/data/pagamentos/"
  output: "./data/output/relatorio_pedidos_recusados_legitimos"

filters:
  ano: 2025
  status_pagamento: false
  fraude: false
```

---

## Execução

Execute a partir da raiz do projeto:

```bash
python src/relatorios_pedidos/main.py
```

O relatório será gravado em `data/output/relatorio_pedidos_recusados_legitimos/` no formato Parquet.

O log de execução também é salvo em `relatorio_pedidos.log` na raiz.

---

## Testes

Execute os testes unitários com:

```bash
pytest tests/ -v
```

Para relatório de cobertura:

```bash
pytest tests/ -v --cov=src/relatorios_pedidos --cov-report=term-missing
```

---

## Arquitetura

O projeto segue os princípios de **Orientação a Objetos** e **Injeção de Dependências**:

| Componente | Classe | Responsabilidade |
|---|---|---|
| Configuração | `Settings` | Carrega e expõe o `settings.yaml` |
| Sessão Spark | `SparkSessionManager` | Cria e encerra a `SparkSession` |
| I/O | `DataHandler` | Lê CSVs/JSONs e grava Parquet com schemas explícitos |
| Lógica de negócio | `Transformation` | Filtros, cálculos e JOIN |
| Orquestração | `Pipeline` | Sequencia as etapas do pipeline |
| Entry point | `main.py` | Aggregation Root — instancia e injeta todas as dependências |

---

## Tecnologias

- [PySpark](https://spark.apache.org/docs/latest/api/python/) 3.5+
- [PyYAML](https://pyyaml.org/) 6.0+
- [pytest](https://pytest.org/) 8.0+
