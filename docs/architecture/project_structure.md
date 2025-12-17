# Estrutura do Projeto CAMO-Net Brasil (AMR Data Engineering)

Este documento descreve a organização de diretórios do projeto de Engenharia de Dados para vigilância de Resistência Antimicrobiana (AMR) em São Caetano do Sul.

A estrutura segue os princípios de **DataOps** e **Arquitetura Medalhão** (Raw -> Bronze -> Silver -> Gold).

## Árvore de Diretórios

```text
camo-net-brasil-amr/
│
├── .github/                    # Workflows de CI/CD (GitHub Actions)
│   └── workflows/              # Pipelines de testes e deploy automatizados
│
├── .dvc/                       # Configuração do Data Version Control (DVC)
│
├── data/                       # ARMAZENAMENTO DE DADOS (Gerenciado via DVC)
│   ├── raw/                    # [Landing Zone] Arquivos imutáveis (csv, xml, xlsx) da fonte.
│   │   # ATENÇÃO: Esta pasta é Read-Only. Nunca alterar arquivos aqui.
│   │
│   ├── bronze/                 # [Ingestion Layer] Dados brutos convertidos (Parquet).
│   │   # Adição de metadados técnicos (_load_date, _source).
│   │
│   ├── silver/                 # [Refined Layer] Dados limpos e padronizados.
│   │   # Processos: Anonimização (LGPD), De-duplicação, Padronização (ATC/CID).
│   │
│   ├── gold/                   # [Business Layer] Tabelas analíticas agregadas.
│   │   # Dados prontos para consumo do Dashboard (Streamlit) e relatórios.
│   │
│   └── external/               # Dados de Referência (Lookups).
│       # Ex: Tabelas CID-10, Classificação ATC, População IBGE.
│
├── docs/                       # Documentação do Projeto
│   ├── architecture/           # Desenhos de arquitetura e este arquivo de estrutura
│   ├── data_dictionary/        # Dicionário de dados (Significado das colunas)
│   ├── privacy/                # Protocolos de ética e LGPD
│   └── publications/        	# Artigos e publicações do projeto
│
├── infrastructure/             # Infraestrutura como Código (IaC)
│   ├── docker/                 # Dockerfiles (App e ETL)
│   └── docker-compose.yml      # Orquestração de containers locais
│
├── notebooks/                  # Sandbox para Experimentação
│   ├── 01_exploratory/         # Análises exploratórias iniciais
│   └── 03_prototyping_ui/      # Rascunhos de visualizações
│   # NOTA: Código de produção não deve ficar aqui. Mover para src/.
│
├── src/                        # CÓDIGO FONTE (Produção)
│   ├── config.py               # Configurações globais e env vars
│   ├── connectors/             # Conexões com Bancos e APIs
│   ├── etl/                    # Pipelines de transformação (Raw -> Gold)
│   ├── dashboard/              # Aplicação Streamlit (Front-end)
│   └── quality/                # Testes de qualidade de dados (Great Expectations)
│
├── tests/                      # Testes Automatizados (Unitários e de Integração)
├── .env.example                # Template de variáveis de ambiente
├── Makefile                    # Atalhos de comando (make run, make test)
├── pyproject.toml              # Gerenciamento de dependências (Poetry/UV)
└── README.md                   # Visão geral do projeto