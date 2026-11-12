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
| `max_brands` | Maximo de marcas (0 = todas) | `2` |
| `max_models` | Maximo de modelos por marca (0 = todos) | `3` |
| `max_years` | Maximo de anos por modelo (0 = todos) | `0` |
| `sleep_seconds` | Pausa entre requisicoes | `0.5` |
| `table` / destino | Tabela de destino | `main.default.fipe_prices` |

Os valores padrao fazem uma execucao de teste pequena. Use `0` nos limites para fazer o crawl completo, que gera milhares de requisicoes.

## Recursos

* Retry com backoff exponencial para respostas 429, 5xx e erros de rede
* Escrita idempotente (merge), sem duplicar registros
* Celula de validacao com contagem de linhas, chaves distintas e precos nulos
