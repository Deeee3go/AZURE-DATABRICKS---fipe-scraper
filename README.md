# AZURE-DATABRICKS---fipe-scraper
Scraper da tabela FIPE a partir da API publica brasileira ([parallelum.com.br](https://parallelum.com.br/fipe/api/v1)), executado em Azure Databricks.

## O que faz

Percorre marcas -> modelos -> anos -> preco, converte o valor para numerico (BRL) e grava o resultado com MERGE em uma tabela Delta, usando como chave `fipe_code + model_year + reference_month`. Se o workspace nao tiver um metastore do Unity Catalog, grava em Parquet dentro dos workspace files.

## Como usar

1. Importe este repositorio como Git folder no Databricks.
2. Abra o notebook `fipe_scraper` e configure os widgets:

| Widget | Descricao | Padrao |
| --- | --- | --- |
| `vehicle_type` | `carros`, `motos` ou `caminhoes` | `carros` |
| `brand_filter` | Filtra por nome ou codigo da marca | vazio |
| `model_filter` | Filtra modelos cujo nome contem o termo (varios termos separados por virgula) | vazio |
| `max_brands` | Maximo de marcas (0 = todas) | `2` |
| `max_models` | Maximo de modelos por marca (0 = todos) | `3` |
| `max_years` | Maximo de anos por modelo (0 = todos) | `0` |
| `sleep_seconds` | Pausa entre requisicoes | `0.5` |
| `table` / destino | Tabela de destino | `main.default.fipe_prices` |

Exemplo: todos os anos do Honda Fit -> `brand_filter=honda`, `model_filter=fit`, `max_brands=0`, `max_models=0`, `max_years=0`.

Os valores padrao fazem uma execucao de teste pequena. Use `0` nos limites para fazer o crawl completo, que gera milhares de requisicoes.

## Modelo estrela (para visualizações)

Após a coleta, o notebook monta um modelo estrela a partir dos preços:

| Tabela | Conteúdo |
| --- | --- |
| `fato_preco_fipe` | 1 linha por código FIPE + ano-modelo + mês de referência: preço (BRL), idade, chaves |
| `dim_marca` | tipo de veículo, código e nome da marca |
| `dim_modelo` | modelo, família (ex.: Fit), motor, câmbio |
| `dim_ano_modelo` | ano-modelo e indicador 0 km |
| `dim_combustivel` | combustível |
| `dim_tempo` | mês de referência como data, ano e mês |

Chaves substitutas determinísticas (`xxhash64`). Com Unity Catalog grava em Delta (`catalog.schema`); sem metastore grava Parquet em `fipe_output/star/` e registra temp views. Inclui um exemplo de visualização: preço médio do Honda Fit por ano-modelo e câmbio.

## Recursos

* Retry com backoff exponencial para respostas 429, 5xx e erros de rede
* Escrita idempotente (merge), sem duplicar registros
* Celula de validacao com contagem de linhas, chaves distintas e precos nulos
