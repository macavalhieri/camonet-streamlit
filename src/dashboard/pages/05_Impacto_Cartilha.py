# src/dashboard/pages/05_Impacto_Cartilha.py
from __future__ import annotations

from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

import pandas as pd
import streamlit as st
import plotly.express as px

from dashboard.data.loaders import load_gold_data
from dashboard.features.builders import build_attendance_level_df


# =============================================================================
# CONFIG
# =============================================================================
st.set_page_config(
    page_title='CAMO-NET',
    page_icon='🦠',
)

st.title('Impacto da Cartilha — Aderência ao guia de antibióticos (pré vs pós)')
st.caption(
    'Comparativo pré vs pós a partir de uma data de corte selecionada. '
    'Unidade de análise: registro (df_raw), filtrado por atendimentos (df_att). '
    'Indicadores de “no guia” dependem de uma lista de compostos (cartilha).'
)

# =============================================================================
# CARTILHA (MVP)
# Preencha com os compostos conforme cartilha (normalizados em lowercase).
# Ex.: {'amoxicilina', 'azitromicina', ...}
# =============================================================================
LISTA_ATB_GUIA: set[str] = set()


# =============================================================================
# HELPERS
# =============================================================================
@st.cache_data(show_spinner=False)
def _load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    df = load_gold_data()
    if df.empty:
        return df, df
    df_att = build_attendance_level_df(df)
    return df, df_att


def _safe_dt_range(series: pd.Series) -> tuple[pd.Timestamp, pd.Timestamp]:
    s = pd.to_datetime(series, errors='coerce').dropna()
    if s.empty:
        today = pd.Timestamp.today().normalize()
        return today - pd.Timedelta(days=30), today
    return s.min().normalize(), s.max().normalize()


def _format_int(n: int) -> str:
    return f'{int(n):,}'.replace(',', '.')


def _format_pct01(x: float) -> str:
    # x em [0,1]
    try:
        return f'{x:.1%}'.replace('.', ',')
    except Exception:
        return '—'


def _apply_filters(
    df_raw: pd.DataFrame,
    df_att: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    dt_min, dt_max = _safe_dt_range(df_att['data_atendimento'])
    min_d, max_d = dt_min.date(), dt_max.date()

    with st.sidebar:
        st.subheader('Filtros')

        st.markdown('Data Início:')
        d_start = st.date_input(
            ' ',
            value=st.session_state.get('flt_start_cart', min_d),
            min_value=min_d,
            max_value=max_d,
            label_visibility='collapsed',
            key='flt_start_cart'
        )

        st.markdown('Data Fim:')
        d_end = st.date_input(
            ' ',
            value=st.session_state.get('flt_end_cart', max_d),
            min_value=min_d,
            max_value=max_d,
            label_visibility='collapsed',
            key='flt_end_cart'
        )

        if d_start > d_end:
            st.warning('Data início maior que data fim. Ajustando automaticamente.')
            d_start, d_end = d_end, d_start

        st.divider()

        st.subheader('Intervenção (cartilha)')
        corte = st.date_input(
            'Data de corte (intervenção)',
            value=st.session_state.get('flt_cut_cart', max_d),
            min_value=min_d,
            max_value=max_d,
            key='flt_cut_cart',
            help='Define o comparativo: Pré (< corte) vs Pós (≥ corte).'
        )

        st.divider()

        unidades = sorted([x for x in df_att['nome_unidade'].dropna().unique().tolist()])
        especialidades = sorted([x for x in df_att['especialidade'].dropna().unique().tolist()])
        faixas = sorted([x for x in df_att['faixa_etaria'].dropna().unique().tolist()])

        sel_unidades = st.multiselect('Unidade de saúde', options=unidades, default=[])
        sel_especialidades = st.multiselect('Especialidade', options=especialidades, default=[])
        sel_faixas = st.multiselect('Faixa etária', options=faixas, default=[])

        sexo_opts = ['Todos', 'Masculino', 'Feminino']
        sel_sexo = st.selectbox('Sexo', options=sexo_opts, index=0)

        st.divider()
        st.subheader('Parâmetros')

        diag_dim_map = {
            'Diagnóstico (agrupado)': 'diag_agrupado',
            'Diagnóstico (análise)': 'diag_analise',
            'Código do diagnóstico (CID/CIAP)': 'cod_cid_ciap',
        }
        diag_dim_label = st.selectbox(
            'Dimensão do diagnóstico',
            list(diag_dim_map.keys()),
            index=0,
        )
        diag_dim = diag_dim_map[diag_dim_label]

        atb_dim_map = {
            'Composto químico (princípio ativo)': 'composto_quimico',
            'Medicamento (nome comercial)': 'nome_medicamento',
            'Código do medicamento': 'cod_medicamento',
        }
        atb_dim_label = st.selectbox(
            'Dimensão do antibiótico',
            list(atb_dim_map.keys()),
            index=0,
        )
        atb_dim = atb_dim_map[atb_dim_label]

        colA, colB = st.columns([0.55, 0.45])
        with colA:
            somente_atb = st.toggle('Somente registros de ATB', value=True)
        with colB:
            somente_infeccioso = st.toggle('Somente diagnóstico infeccioso', value=False)

        st.caption('Dica: para aderência à cartilha, use “Somente registros de ATB”.')

    # Filtro base em atendimento (recorte populacional)
    df_att_f = df_att.copy()
    df_att_f['data_atendimento'] = pd.to_datetime(df_att_f['data_atendimento'], errors='coerce')
    df_att_f = df_att_f[
        df_att_f['data_atendimento'].notna() &
        (df_att_f['data_atendimento'].dt.date >= d_start) &
        (df_att_f['data_atendimento'].dt.date <= d_end)
    ]

    if sel_unidades:
        df_att_f = df_att_f[df_att_f['nome_unidade'].isin(sel_unidades)]
    if sel_especialidades:
        df_att_f = df_att_f[df_att_f['especialidade'].isin(sel_especialidades)]
    if sel_faixas:
        df_att_f = df_att_f[df_att_f['faixa_etaria'].isin(sel_faixas)]
    if sel_sexo != 'Todos':
        target = 'm' if sel_sexo == 'Masculino' else 'f'
        df_att_f = df_att_f[df_att_f['sexo'].astype(str).str.lower() == target]

    # Filtra df_raw pelo conjunto de atendimentos final
    ids = df_att_f['cod_atendimento'].astype(str).unique().tolist()
    df_raw_f = df_raw[df_raw['cod_atendimento'].astype(str).isin(ids)].copy()

    # Normalizações mínimas
    df_raw_f['data_atendimento'] = pd.to_datetime(df_raw_f['data_atendimento'], errors='coerce')
    df_raw_f['e_antibiotico'] = pd.to_numeric(df_raw_f.get('e_antibiotico', 0), errors='coerce').fillna(0).astype(int)
    df_raw_f['e_diag_infeccioso'] = pd.to_numeric(df_raw_f.get('e_diag_infeccioso', 0), errors='coerce').fillna(0).astype(int)

    if somente_atb:
        df_raw_f = df_raw_f[df_raw_f['e_antibiotico'] == 1].copy()

    if somente_infeccioso:
        df_raw_f = df_raw_f[df_raw_f['e_diag_infeccioso'] == 1].copy()

    params = {
        'diag_dim': diag_dim,
        'atb_dim': atb_dim,
        'd_start': d_start,
        'd_end': d_end,
        'corte': pd.Timestamp(corte),
        'somente_atb': somente_atb,
        'somente_infeccioso': somente_infeccioso,
    }
    return df_raw_f, df_att_f, params


def _safe_pct_true_bool(s: pd.Series) -> float:
    s = s.dropna()
    if s.empty:
        return 0.0
    return float(s.astype(bool).mean())


# =============================================================================
# LOAD
# =============================================================================
with st.spinner('Carregando dados...'):
    df_raw_base, df_att_base = _load_data()

if df_raw_base.empty or df_att_base.empty:
    st.error('Não foi possível carregar os dados (DataFrame vazio).')
    st.stop()

df_raw, df_att, params = _apply_filters(df_raw_base, df_att_base)

diag_dim = params['diag_dim']
atb_dim = params['atb_dim']
corte = params['corte']
somente_atb = params['somente_atb']
somente_infeccioso = params['somente_infeccioso']

# =============================================================================
# FEATURE: no guia (cartilha) — baseada em composto_quimico (recomendado)
# =============================================================================
if LISTA_ATB_GUIA:
    df_raw['e_no_guia'] = (
        df_raw['composto_quimico']
        .astype(str)
        .str.strip()
        .str.lower()
        .isin(LISTA_ATB_GUIA)
    )
else:
    df_raw['e_no_guia'] = pd.NA

df_raw['periodo'] = pd.NA
df_raw.loc[df_raw['data_atendimento'].notna() & (df_raw['data_atendimento'] < corte), 'periodo'] = 'Pré'
df_raw.loc[df_raw['data_atendimento'].notna() & (df_raw['data_atendimento'] >= corte), 'periodo'] = 'Pós'

pre = df_raw[df_raw['periodo'] == 'Pré'].copy()
pos = df_raw[df_raw['periodo'] == 'Pós'].copy()

# =============================================================================
# KPIs
# =============================================================================
st.subheader('KPIs — pré vs pós (no recorte atual)')

k1, k2, k3, k4, k5, k6 = st.columns(6)
with k1:
    st.metric('Atendimentos (recorte)', _format_int(int(df_att.shape[0])))
with k2:
    st.metric('Registros (Pré)', _format_int(int(pre.shape[0])))
with k3:
    st.metric('Registros (Pós)', _format_int(int(pos.shape[0])))
with k4:
    st.metric('Corte', str(corte.date()))

if LISTA_ATB_GUIA:
    with k5:
        st.metric('% no guia (Pré)', _format_pct01(_safe_pct_true_bool(pre['e_no_guia'])))
    with k6:
        st.metric('% no guia (Pós)', _format_pct01(_safe_pct_true_bool(pos['e_no_guia'])))
else:
    with k5:
        st.metric('% no guia (Pré)', '—')
    with k6:
        st.metric('% no guia (Pós)', '—')

    st.warning(
        'LISTA_ATB_GUIA está vazia. '
        'Preencha com os compostos (lowercase) para habilitar os indicadores “no guia”.'
    )

st.divider()

# =============================================================================
# MAIN
# =============================================================================
tab1, tab2 = st.tabs(['Visão Analítica', 'Inspeção'])

with tab1:
    st.subheader('Aderência à cartilha — Pré vs Pós')

    if not LISTA_ATB_GUIA:
        st.info('Configure LISTA_ATB_GUIA para habilitar esta seção.')
    else:
        bar_df = (
            df_raw.assign(status_cartilha=lambda d: d['e_no_guia'].map({True: 'No guia', False: 'Fora do guia'}))
                  .groupby(['periodo', 'status_cartilha'], as_index=False)
                  .size()
                  .rename(columns={'size': 'n'})
        )
        bar_df['pct'] = bar_df.groupby('periodo')['n'].transform(lambda x: (x / x.sum()) if x.sum() else 0.0)

        fig_bar = px.bar(
            bar_df,
            x='periodo',
            y='pct',
            color='status_cartilha',
            barmode='group',
            text='pct',
            labels={'pct': 'Percentage', 'periodo': 'Período', 'status_cartilha': 'Status'},
        )
        fig_bar.update_traces(texttemplate='%{text:.1%}', textposition='outside')
        fig_bar.update_layout(height=420, margin=dict(l=20, r=20, t=40, b=20))
        fig_bar.update_yaxes(range=[0, 1])
        st.plotly_chart(fig_bar, use_container_width=True)

        st.caption(
            f'Períodos: Pré (< {corte.date()}) vs Pós (≥ {corte.date()}). '
            f'Universo: {"somente ATB" if somente_atb else "todos os registros"}'
            f'{"; somente infeccioso" if somente_infeccioso else ""}.'
        )

    st.divider()
    st.subheader('Tendência mensal — % fora do guia')

    if not LISTA_ATB_GUIA:
        st.info('Configure LISTA_ATB_GUIA para habilitar tendência mensal.')
    else:
        tmp = df_raw.copy()
        tmp = tmp[tmp['data_atendimento'].notna()].copy()
        tmp['ano_mes_plot'] = tmp['data_atendimento'].dt.to_period('M').astype(str)

        trend = (
            tmp.assign(fora_guia=lambda d: (~d['e_no_guia']).astype(int))
               .groupby('ano_mes_plot', as_index=False)
               .agg(
                   n=('cod_atendimento', 'count'),
                   pct_fora=('fora_guia', lambda x: float(x.mean()) if len(x) else 0.0),
               )
               .sort_values('ano_mes_plot')
        )

        fig_trend = px.line(
            trend,
            x='ano_mes_plot',
            y='pct_fora',
            markers=True,
            labels={'pct_fora': 'Percentage', 'ano_mes_plot': 'Ano-Mês'},
        )
        fig_trend.update_yaxes(tickformat='.1%')
        fig_trend.update_layout(height=420, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_trend, use_container_width=True)

        st.caption('Observação: a linha vertical da data de corte pode ser inferida visualmente pelo comparativo Pré vs Pós.')

    st.divider()
    st.subheader('Ranking — antibióticos fora do guia')

    if not LISTA_ATB_GUIA:
        st.info('Configure LISTA_ATB_GUIA para habilitar ranking.')
    else:
        rank = (
            df_raw[df_raw['e_no_guia'] == False]
            .groupby(atb_dim, as_index=False)
            .agg(registros=('cod_atendimento', 'count'), atendimentos=('cod_atendimento', 'nunique'))
            .sort_values('registros', ascending=False)
            .head(15)
        )

        if rank.empty:
            st.info('Nenhum registro fora do guia no recorte atual.')
        else:
            fig_rank = px.bar(
                rank,
                x='registros',
                y=atb_dim,
                orientation='h',
                hover_data={'atendimentos': True},
                labels={'registros': 'Registros', atb_dim: 'Antibiótico'},
            )
            fig_rank.update_layout(height=520, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_rank, use_container_width=True)

            st.caption('Ranking por volume de registros; o hover mostra atendimentos únicos afetados.')

with tab2:
    st.subheader('Inspeção de registros (df_raw) no recorte atual')
    st.caption(
        'Use esta aba para validar registros específicos (ex.: por que um ATB foi classificado fora do guia).'
    )

    cols_show = [
        'cod_atendimento',
        'data_atendimento',
        'nome_unidade',
        'especialidade',
        'cod_cid_ciap',
        'diag_agrupado',
        'diag_analise',
        'e_diag_infeccioso',
        'cod_medicamento',
        'nome_medicamento',
        'composto_quimico',
        'concentracao',
        'unidade_apresentacao',
        'duracao',
        'e_antibiotico',
        'e_presc_inadequada',
        'e_no_guia',
        'periodo',
    ]
    cols_show = [c for c in cols_show if c in df_raw.columns]

    st.dataframe(
        df_raw[cols_show].sort_values('data_atendimento', ascending=False).head(5000),
        use_container_width=True,
        height=420
    )

    st.divider()
    st.subheader('Detalhe por atendimento')

    options = (
        df_raw['cod_atendimento']
        .astype(str)
        .dropna()
        .unique()
        .tolist()
    )
    chosen = st.selectbox(
        'Selecione um cod_atendimento para inspecionar',
        options=options[:5000],
        index=0 if options else None
    )

    if chosen:
        df_detail = df_raw[df_raw['cod_atendimento'].astype(str) == str(chosen)].copy()
        df_detail = df_detail.sort_values('data_atendimento', ascending=False)

        cols_detail = [c for c in cols_show if c in df_detail.columns]
        st.dataframe(df_detail[cols_detail], use_container_width=True, height=360)