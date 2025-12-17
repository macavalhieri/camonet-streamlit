# Streamlit Deployment — CAMO-Net v4

Este documento descreve o processo, as decisões arquiteturais e as boas práticas adotadas para transformar o dashboard analítico do **CAMO-Net** de um ambiente local (notebooks) em uma **aplicação web funcional**, publicada via **Streamlit Community Cloud**, sem exposição de dados sensíveis.

O objetivo é servir como:
- documentação técnica do projeto;
- guia de onboarding para novos pesquisadores;
- referência para projetos analíticos em saúde com requisitos de governança, reprodutibilidade e LGPD.

---

## 1. Contexto

O projeto CAMO-Net iniciou-se com análises exploratórias em notebooks locais, focadas em vigilância do uso de antimicrobianos e identificação de cenários de inadequação terapêutica.

Com a evolução do projeto, tornou-se necessário:
- compartilhar resultados com pesquisadores e gestores;
- permitir exploração interativa dos dados;
- garantir reprodutibilidade;
- manter conformidade com LGPD e protocolos institucionais.

A solução adotada foi a publicação de um **dashboard web em Streamlit**, com separação clara entre **código**, **dados** e **configurações sensíveis**.

---

## 2. Estrutura do Projeto

```text
camonet_v4/
├── src/
│   └── dashboard/
│       ├── Home.py                 # Entry point do Streamlit
│       ├── pages/                  # Páginas multipage do dashboard
│       ├── data/                   # Loaders de dados (apenas código)
│       ├── features/               # Regras analíticas e agregações
│       └── viz/                    # Componentes de visualização reutilizáveis
│
├── docs/
│   ├── architecture/               # Documentação técnica
│   ├── methodology/                # Premissas e limitações
│   ├── backlog/                    # Backlog metodológico
│   └── references/                 # Artigos/cartilhas (não versionados)
│
├── requirements.txt
├── runtime.txt
└── README.md
```

### Princípios adotados
- Código ≠ dados
- Arquitetura explícita e auditável
- Facilidade de manutenção
- Compatibilidade com Streamlit Cloud

---

## 3. Adaptação do Código para Streamlit

### 3.1 Imports e PYTHONPATH

Como o projeto utiliza layout `src/`, foi necessário garantir que o pacote `dashboard` fosse resolvido corretamente no Streamlit Cloud.

Cada página inclui, no topo:

```python
from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))
```

Isso garante:
- imports consistentes;
- funcionamento local e em produção;
- ausência de dependência de instalação do pacote via `pip`.

---

### 3.2 Separação de responsabilidades

- **data/**: apenas carregamento de dados (I/O)
- **features/**: regras analíticas e agregações
- **pages/**: interface, filtros e visualizações

Essa separação permite:
- reuso do código analítico em notebooks e artigos;
- auditoria metodológica;
- evolução incremental do dashboard.

---

## 4. Gestão de Dados (fora do Git)

### 4.1 Princípio fundamental

> **Dados nunca são versionados no repositório.**

O repositório contém apenas:
- código;
- documentação;
- configuração de ambiente.

Bases analíticas são carregadas externamente.

---

### 4.2 Uso do Google Drive (link direto)

Para esta versão do projeto, optou-se por armazenar o dataset Gold (`.parquet`) no Google Drive, acessado via **link de download direto**, configurado como secret no Streamlit.

```toml
DATA_URL = "https://drive.google.com/uc?export=download&id=FILE_ID"
```

Características:
- dataset fora do GitHub;
- acesso apenas via URL conhecida;
- menor exposição do que assets públicos no repositório;
- simplicidade operacional para datasets pequenos/moderados.

---

### 4.3 Loader com cache local

```python
def load_gold_data():
    if not CACHE_FILE.exists():
        download_from_url()
    return pd.read_parquet(CACHE_FILE)
```

Benefícios:
- melhor performance;
- menor dependência de rede;
- maior estabilidade em ambiente serverless.

---

## 5. Uso de Secrets

Credenciais e URLs sensíveis são geridas via:
- `st.secrets` no Streamlit Community Cloud;
- `.streamlit/secrets.toml` no ambiente local (não versionado).

---

## 6. `.gitignore` e Proteção de Conteúdo Sensível

```gitignore
/data/
/data/**

!/src/dashboard/data/
!/src/dashboard/data/**
```

---

## 7. Deploy no Streamlit Community Cloud

- Repository: GitHub (repo público)
- Branch: main
- Main file path: `src/dashboard/Home.py`
- Python version: `runtime.txt`

---

## 8. Versionamento e Releases

A versão `v4.0.0` marca:
- primeiro deploy público funcional;
- arquitetura estabilizada;
- integração correta com dados externos.

---

## 9. Conformidade, Ética e LGPD

- Nenhum dado pessoal é versionado.
- Uso voltado à vigilância populacional.
- Limitações documentadas em `docs/methodology/`.

---

## 10. Lições Aprendidas

- Separar código e dados evita retrabalho.
- `.gitignore` mal configurado quebra imports.
- Documentação é parte do produto.

---

## 11. Próximos Passos

- Guideline clínico
- Indicadores de stewardship
- Automatização de atualização de dados

---

## 12. Conclusão

Este documento consolida a experiência de transformar um projeto de pesquisa em um produto analítico web, mantendo rigor técnico, ético e institucional.
