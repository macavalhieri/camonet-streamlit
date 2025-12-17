# CAMO-Net Brasil — Portal de Vigilância em Resistência Antimicrobiana (AMR)

Este repositório contém o código do **dashboard interativo do projeto CAMO-Net Brasil**, desenvolvido para apoiar a análise e vigilância do uso de antimicrobianos na Atenção Primária à Saúde, com foco em indicadores de prescrição, adequação terapêutica e resistência antimicrobiana.

O dashboard foi construído em **Streamlit**, com arquitetura modular e documentação metodológica voltada para **reprodutibilidade científica**, **governança de dados** e **uso por pesquisadores e gestores de saúde**.

---

## Objetivo do Projeto

O CAMO-Net tem como objetivo:

- Monitorar padrões de prescrição de antibióticos
- Identificar **cenários de descompasso** entre diagnóstico infeccioso e uso de antimicrobianos
- Apoiar estratégias de **antimicrobial stewardship**
- Fornecer evidências para vigilância epidemiológica e tomada de decisão

> ⚠️ Este sistema é destinado à **análise populacional e vigilância**, não à avaliação clínica individual.

---

## Data Privacy

This repository contains only source code and documentation.
No personal, sensitive or operational data from CAMO-Net are included.

## Estrutura do Repositório

```text
camonet_v4/
├── src/
│   └── dashboard/
│       ├── Home.py                 # Página inicial (entrypoint Streamlit)
│       ├── pages/                  # Páginas multipage do dashboard
│       ├── data/                   # Loaders de dados (camada Gold)
│       ├── features/               # Regras analíticas e agregações
│       └── viz/                    # Componentes de visualização reutilizáveis
│
├── docs/
│   ├── methodology/                # Premissas, limitações e regras analíticas
│   ├── backlog/                    # Backlog metodológico e roadmap
│   ├── references/                 # Artigos, cartilhas e protocolos clínicos
│   ├── slides/                     # Apresentações e demos
│   └── supplements/                # Materiais complementares
│
├── requirements.txt                # Dependências do dashboard
├── runtime.txt                     # Versão do Python (Streamlit Cloud)
└── README.md

---

