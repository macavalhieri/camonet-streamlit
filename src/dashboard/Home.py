from __future__ import annotations
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))


import pandas as pd
import streamlit as st

from dashboard.data.loaders import load_gold_data
from dashboard.features.builders import build_attendance_level_df


# =============================================================================
# CONFIG
# =============================================================================
st.set_page_config(
    page_title='CAMO-NET',
    page_icon='🦠',
    layout='wide',
    initial_sidebar_state='expanded',
)


# =============================================================================
# DATA (cached)
# =============================================================================
@st.cache_data(show_spinner=False)
def _load_home_metrics() -> dict:
    df = load_gold_data()
    if df.empty:
        return {
            'ok': False,
            'error': 'Dataset vazio (Gold). Verifique a origem/arquivo de dados.',
        }

    # Garantir datetime para cálculo de min/max
    dt = pd.to_datetime(df['data_atendimento'], errors='coerce')

    n_prescricoes = int(len(df))
    n_atendimentos = int(df['cod_atendimento'].nunique())

    # Se quiser consistência com a visão agregada por atendimento:
    # df_att = build_attendance_level_df(df)
    # n_atendimentos = int(len(df_att))

    return {
        'ok': True,
        'n_prescricoes': n_prescricoes,
        'n_atendimentos': n_atendimentos,
        'dt_min': dt.min(),
        'dt_max': dt.max(),
    }


# =============================================================================
# PAGE
# =============================================================================
st.title('CAMO-Net Brasil | Portal de Vigilância AMR')

try:
    metrics = _load_home_metrics()
except Exception as e:
    st.error(f'Falha ao carregar dados da camada Gold: {e}')
    st.stop()

if not metrics.get('ok', False):
    st.warning(metrics.get('error', 'Não foi possível carregar as métricas do dataset.'))
    st.stop()

c1, c2, c3, c4 = st.columns(4)

c1.metric('Prescrições (linhas)', f"{metrics['n_prescricoes']:,}".replace(',', '.'))
c2.metric('Atendimentos únicos', f"{metrics['n_atendimentos']:,}".replace(',', '.'))

dt_min = metrics['dt_min']
dt_max = metrics['dt_max']

c3.metric('Data mínima', dt_min.date().isoformat() if pd.notna(dt_min) else '—')
c4.metric('Data máxima', dt_max.date().isoformat() if pd.notna(dt_max) else '—')

st.markdown(
    """
### Bem-vindo ao Portal

Este portal disponibiliza módulos para análise e vigilância do uso de antimicrobianos na
Atenção Primária, com foco em indicadores de prescrição e sinais de inadequação
associados a diagnósticos infecciosos.

### Módulos

Use a barra lateral para navegar entre as páginas disponíveis, incluindo:

- **Atendimentos**: métricas no nível de atendimento (cod_atendimento) e segmentações
- **Antibióticos**: padrões de prescrição e composição terapêutica
- **Inadequações**: análises do **descompasso** entre diagnóstico infeccioso e prescrição de antibiótico (ex.: ATB sem CID infeccioso), com recortes por especialidade, unidade e perfil do paciente
- **Análise Antimicrobiana**: análises de atendimentos com **diagnóstico infeccioso e prescrição de antibiótico** (alinhamento infecção–ATB), incluindo padrões de tratamento, segmentações e exploração orientada

---

**Nota de conformidade:** este projecto segue protocolos de pesquisa e princípios da LGPD.
"""
)

st.sidebar.info('Navegue pelos módulos na barra lateral.')
