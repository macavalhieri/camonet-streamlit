# Índice de Documentação - CAMO-Net Architecture

## 📚 Documentos Disponíveis

### Arquitetura e Estrutura

1. **[Estrutura do Projeto](file:///home/mac/professional/camonet_v3/docs/architecture/project_structure.md)**
   - Organização de diretórios
   - Arquitetura Medalhão (Raw → Bronze → Silver → Gold)
   - Convenções de código e dados

2. **[Relatório de Validação de Dados](file:///home/mac/professional/camonet_v3/docs/architecture/data_validation_report.md)**
   - Comparação com artigo publicado
   - Validação de métricas-chave
   - Análise de divergências
   - Metodologia de validação

### Modelo de Dados

#### Camada Gold (Modelo Dimensional)

**Dimensões**:
- `dim_tempo`: 225 datas (Jan-Set 2023)
- `dim_unidade_saude`: 50 unidades de atenção primária
- `dim_atendimento`: 480,439 atendimentos únicos
- `dim_paciente`: 67,023 pacientes (anonimizados)
- `dim_medicamento`: 33,246 medicamentos (com classificação WHO AWaRe)
- `dim_diagnostico`: 1,483 diagnósticos (CID + CIAP)

**Fatos**:
- `fato_prescricao`: 306,318 prescrições (granularidade: 1 prescrição)
- `fato_diagnostico`: 298,848 diagnósticos (granularidade: 1 diagnóstico)
- `fato_atendimento_resumo`: 480,439 atendimentos (granularidade: 1 atendimento)

### Scripts ETL

1. **[00_data_profiling.py](file:///home/mac/professional/camonet_v3/src/etl/00_data_profiling.py)**
   - Profiling de qualidade de dados
   - Identificação de problemas (nulos, duplicatas, tipos)

2. **[01_raw_to_bronze.py](file:///home/mac/professional/camonet_v3/src/etl/01_raw_to_bronze.py)**
   - Conversão CSV → Parquet
   - Adição de metadados de ingestão

3. **[02_bronze_to_silver.py](file:///home/mac/professional/camonet_v3/src/etl/02_bronze_to_silver.py)**
   - Padronização (snake_case)
   - Limpeza de dados
   - Anonimização LGPD

4. **[03_silver_to_gold.py](file:///home/mac/professional/camonet_v3/src/etl/03_silver_to_gold.py)**
   - Criação de modelo dimensional
   - Geração de surrogate keys
   - Classificação WHO AWaRe
   - Validação de integridade referencial

### Análises e Notebooks

1. **[01_initial_analysis.ipynb](file:///home/mac/professional/camonet_v3/notebooks/01_exploratory/01_initial_analysis.ipynb)**
   - EDA automática
   - Análise de chaves de relacionamento
   - Top 20 diagnósticos e medicamentos
   - Variações de antibióticos

### Publicações

1. **[Artigo Ana Roccio - Análise Antimicrobiana](file:///home/mac/professional/camonet_v3/docs/publications/artigo_ana_roccio_analise_antimicrobiana.pdf)**
   - Maita et al. BMC Medical Informatics and Decision Making (2025)
   - Referência principal do projeto

---

## 🔍 Navegação Rápida

### Por Camada de Dados

- **Raw**: `data/20250101_carga_inicial_ana_roccio/` (CSV original)
- **Bronze**: `data/bronze/` (Parquet + metadados)
- **Silver**: `data/silver/` (Curado + anonimizado)
- **Gold**: `data/gold/` (Modelo dimensional)

### Por Tipo de Análise

- **Qualidade de Dados**: [data_validation_report.md](file:///home/mac/professional/camonet_v3/docs/architecture/data_validation_report.md)
- **Profiling**: Execute `python3 src/etl/00_data_profiling.py`
- **EDA**: Abra [01_initial_analysis.ipynb](file:///home/mac/professional/camonet_v3/notebooks/01_exploratory/01_initial_analysis.ipynb)

### Por Objetivo AMR

- **Prescrições de Antibióticos**: `fato_prescricao` (8,182 antibióticos)
- **Diagnósticos Infecciosos**: `fato_diagnostico` (16,572 infecções)
- **Adequação de Prescrições**: `fato_prescricao.e_prescricao_apropriada`
- **Classificação WHO AWaRe**: `dim_medicamento.classe_who_aware`

---

## 📊 Métricas-Chave Validadas

| Métrica | Artigo | Gold | Status |
|---------|--------|------|--------|
| Pacientes únicos | 67,023 | 67,023 | ✅ Match |
| Diagnósticos infecciosos | 16,572 | 16,572 | ✅ Match |
| Atendimentos | 575,616* | 480,439 | ⚠️ Granularidade |
| Prescrições antibióticos | 7,938 | 8,182 | ⚠️ Critério |

*Artigo usa granularidade atendimento × diagnóstico

---

## 🚀 Como Usar Esta Documentação

1. **Novos Membros da Equipe**: Comece por [project_structure.md](file:///home/mac/professional/camonet_v3/docs/architecture/project_structure.md)
2. **Validação de Dados**: Consulte [data_validation_report.md](file:///home/mac/professional/camonet_v3/docs/architecture/data_validation_report.md)
3. **Desenvolvimento ETL**: Veja scripts em `src/etl/`
4. **Análises AMR**: Use tabelas em `data/gold/`

---

**Última Atualização**: 2025-11-26  
**Versão**: 1.0  
**Mantido por**: Equipe de Engenharia de Dados CAMO-Net
