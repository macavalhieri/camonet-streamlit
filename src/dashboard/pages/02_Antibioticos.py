# src/dashboard/pages/02_Antibioticos.py
from __future__ import annotations

from pathlib import Path
import sys

SRC_ROOT = Path(__file__).resolve().parents[2]
if str(SRC_ROOT) not in sys.path:
    sys.path.append(str(SRC_ROOT))

import numpy as np
import pandas as pd
import streamlit as st
import plotly.express as px

from dashboard.data.loaders import load_gold_data
from dashboard.features.builders import build_attendance_level_df


# =============================================================================
# CONFIG
# =============================================================================
st.set_page_config(
    page_title="CAMO-NET",
    page_icon="🦠",
)

st.title('Perfil de Uso de Antibióticos')
st.caption(
    'Visão operacional do uso de antibióticos, com indicadores em nível de atendimento '
    '(cod_atendimento) e detalhamento por prescrição quando aplicável.'
)


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


def _format_pct(x: float) -> str:
    return f'{x:.1%}'.replace('.', ',')


def _apply_filters(df_raw: pd.DataFrame, df_att: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Datas (a partir do df_att)
    dt_min, dt_max = _safe_dt_range(df_att['data_atendimento'])
    min_d = dt_min.date()
    max_d = dt_max.date()

    with st.sidebar:
        st.subheader('Filtros')

        st.markdown('Data Início:')
        d_start = st.date_input(
            ' ',
            value=st.session_state.get('flt_start', min_d),
            min_value=min_d,
            max_value=max_d,
            label_visibility='collapsed',
            key='flt_start'
        )

        st.markdown('Data Fim:')
        d_end = st.date_input(
            ' ',
            value=st.session_state.get('flt_end', max_d),
            min_value=min_d,
            max_value=max_d,
            label_visibility='collapsed',
            key='flt_end'
        )

        if d_start > d_end:
            st.warning('Data início maior que data fim. Ajustando automaticamente.')
            d_start, d_end = d_end, d_start

        st.divider()

        # Listas para filtros
        unidades = sorted([x for x in df_att['nome_unidade'].dropna().unique().tolist()])
        especialidades = sorted([x for x in df_att['especialidade'].dropna().unique().tolist()])
        faixas = sorted([x for x in df_att['faixa_etaria'].dropna().unique().tolist()])

        sel_unidades = st.multiselect('Unidade de saúde', options=unidades, default=[])
        sel_especialidades = st.multiselect('Especialidade', options=especialidades, default=[])
        sel_faixas = st.multiselect('Faixa etária', options=faixas, default=[])

        sexo_opts = ['Todos', 'Masculino', 'Feminino']
        sel_sexo = st.selectbox('Sexo', options=sexo_opts, index=0)

        st.divider()

    # Aplica filtros (df_att)
    df_att_f = df_att.copy()
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

    # Para df_raw, filtra pelos atendimentos selecionados (garante consistência)
    ids = df_att_f['cod_atendimento'].astype(str).unique().tolist()
    df_raw_f = df_raw[df_raw['cod_atendimento'].astype(str).isin(ids)].copy()

    return df_raw_f, df_att_f


# =============================================================================
# LOAD
# =============================================================================
with st.spinner('Carregando dados...'):
    df_raw_base, df_att_base = _load_data()

if df_raw_base.empty or df_att_base.empty:
    st.error('Não foi possível carregar os dados (DataFrame vazio).')
    st.stop()

df_raw, df_att = _apply_filters(df_raw_base, df_att_base)


# =============================================================================
# KPIs (nível atendimento)
# =============================================================================
total_atend = int(df_att.shape[0])
total_atend_atb = int((df_att['tem_antibiotico'].fillna(0).astype(int) == 1).sum())
pct_atend_atb = (total_atend_atb / total_atend) if total_atend else 0.0

media_atb_por_atend = float(df_att['n_antibioticos'].fillna(0).mean()) if total_atend else 0.0

total_prescricoes = int(df_raw.shape[0])
total_presc_atb = int((df_raw['e_antibiotico'].fillna(0).astype(int) == 1).sum())
pct_presc_atb = (total_presc_atb / total_prescricoes) if total_prescricoes else 0.0

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    st.metric('Atendimentos', _format_int(total_atend))
with k2:
    st.metric('Atendimentos com ATB', _format_int(total_atend_atb))
with k3:
    st.metric('% atendimentos com ATB', _format_pct(pct_atend_atb))
with k4:
    st.metric('Média ATBs / atendimento', f'{media_atb_por_atend:.2f}'.replace('.', ','))
with k5:
    st.metric('% prescrições que são ATB', _format_pct(pct_presc_atb))

st.divider()


# =============================================================================
# TABS
# =============================================================================
tab1, tab2, tab3 = st.tabs(['Tendência', 'Rankings', 'Medicamentos (ATB)'])

# =============================================================================
# TAB 1: Tendência
# =============================================================================
with tab1:
    tmp = df_att.copy()
    tmp = tmp[tmp['data_atendimento'].notna()].copy()
    tmp['ano_mes'] = tmp['data_atendimento'].dt.to_period('M').astype(str)

    monthly = (
        tmp
        .groupby('ano_mes')
        .agg(
            atendimentos=('cod_atendimento', 'count'),
            atend_atb=('tem_antibiotico', 'sum'),
            soma_atb=('n_antibioticos', 'sum'),
        )
        .reset_index()
    )
    monthly['pct_atend_atb'] = monthly['atend_atb'] / monthly['atendimentos']
    monthly['media_atb_por_atend'] = monthly['soma_atb'] / monthly['atendimentos']

    c1, c2 = st.columns(2)

    with c1:
        st.subheader('Atendimentos')
        fig = px.line(monthly, x='ano_mes', y='pct_atend_atb', markers=True)
        fig.update_yaxes(title='% atendimentos com ATB', tickformat='.0%')
        fig.update_xaxes(title=None)
        fig.update_layout(height=340, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader('Prescrições')
        fig2 = px.line(monthly, x='ano_mes', y='media_atb_por_atend', markers=True)
        fig2.update_yaxes(title='Média de ATBs por atendimento')
        fig2.update_xaxes(title=None)
        fig2.update_layout(height=340, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # -----------------------------------------------------------------------------
    # Tratamento combinado - 2+ ATBs
    # -----------------------------------------------------------------------------
    st.subheader('Tratamento Combinado')

    df_att['ano_mes'] = df_att['data_atendimento'].dt.to_period('M').astype(str)

    cols_base = ['cod_atendimento', 'ano_mes', 'n_antibioticos']
    df_base = df_att[cols_base].copy()
    df_base['n_antibioticos'] = df_base['n_antibioticos'].fillna(0).astype(int)

    # -----------------------------------------------------------------------------
    # KPI (calcula antes para usar no card + sparkline)
    # -----------------------------------------------------------------------------
    kpi = (
        df_base.assign(politerapia=lambda d: d['n_antibioticos'] >= 2)
            .groupby('ano_mes', as_index=False)
            .agg(
                total=('cod_atendimento', 'nunique'),
                politerapia=('politerapia', 'sum')
            )
    )

    kpi['pct_politerapia'] = np.where(
        kpi['total'] > 0,
        100 * kpi['politerapia'] / kpi['total'],
        0.0
    )
    kpi = kpi.sort_values('ano_mes')

    last = kpi.iloc[-1] if not kpi.empty else None
    prev = kpi.iloc[-2]['pct_politerapia'] if len(kpi) > 1 else None
    delta = None if (last is None or prev is None) else float(last['pct_politerapia'] - prev)

    # -----------------------------------------------------------------------------
    # Header controls: Card + Sparkline + Checkbox
    # -----------------------------------------------------------------------------
    col_left, col_right = st.columns(2)

    with col_left:
        st.metric(
            label='Combinado (≥ 2 ATBs)',
            value='—' if last is None else f"{last['pct_politerapia']:.1f}%",
            delta=None if delta is None else f"{delta:+.1f} p.p.",
            help='Percentual de atendimentos com dois ou mais antibióticos'
        )

    with col_right:
        incluir_sem_atb = st.checkbox('Incluir 0 ATB', value=False)

    # -----------------------------------------------------------------------------
    # Classificação (1 ATB vs ≥2 ATBs; opcional: incluir 0)
    # -----------------------------------------------------------------------------
    def _classe_atb(n: int) -> str:
        if n == 0:
            return '0 ATB'
        if n == 1:
            return '1 ATB'
        return '≥2 ATBs'

    df_base['classe_atb'] = df_base['n_antibioticos'].apply(_classe_atb)

    if not incluir_sem_atb:
        df_base = df_base[df_base['n_antibioticos'] > 0].copy()

    # -----------------------------------------------------------------------------
    # Agregação mensal para barras empilhadas
    # -----------------------------------------------------------------------------
    agg = (
        df_base.groupby(['ano_mes', 'classe_atb'], as_index=False)
        .agg(atendimentos=('cod_atendimento', 'nunique'))
    )

    ordem_classes = ['0 ATB', '1 ATB', '≥2 ATBs'] if incluir_sem_atb else ['1 ATB', '≥2 ATBs']
    agg['classe_atb'] = pd.Categorical(agg['classe_atb'], categories=ordem_classes, ordered=True)

    # Completa combinações faltantes (meses sem uma classe específica)
    meses = sorted(agg['ano_mes'].unique())
    idx = pd.MultiIndex.from_product([meses, ordem_classes], names=['ano_mes', 'classe_atb'])
    agg = (
        agg.set_index(['ano_mes', 'classe_atb'])
        .reindex(idx, fill_value=0)
        .reset_index()
    )

    # -----------------------------------------------------------------------------
    # Gráfico (barras empilhadas por mês)
    # -----------------------------------------------------------------------------
    fig = px.bar(
        agg,
        x='ano_mes',
        y='atendimentos',
        color='classe_atb',
        barmode='stack',
        category_orders={'classe_atb': ordem_classes}
    )
    fig.update_xaxes(title='Ano-Mês')
    fig.update_yaxes(title='Atendimentos')
    fig.update_layout(height=340, margin=dict(l=20, r=20, t=40, b=20), legend_title_text='')
    st.plotly_chart(fig, use_container_width=True)

# =============================================================================
# TAB 2: Rankings
# =============================================================================
with tab2:
    st.subheader('Rankings (nível atendimento)')

    colA, colB, colC = st.columns(3)
    with colA:
        min_n = st.slider('Mínimo de atendimentos por grupo', 10, 500, 50, 10)
    with colB:
        top_n = st.slider('Top N', 5, 30, 12, 1)
    with colC:
        metric = st.selectbox(
            'Métrica',
            options=['% atendimentos com ATB', 'Média de ATBs por atendimento'],
            index=0
        )

    # UBS
    df_u = df_att.copy()
    u = (
        df_u
        .groupby('nome_unidade', dropna=False)
        .agg(
            atendimentos=('cod_atendimento', 'count'),
            atend_atb=('tem_antibiotico', 'sum'),
            soma_atb=('n_antibioticos', 'sum')
        )
        .reset_index()
    )
    u['pct_atend_atb'] = u['atend_atb'] / u['atendimentos']
    u['media_atb'] = u['soma_atb'] / u['atendimentos']
    u = u[u['atendimentos'] >= min_n].copy()

    # Especialidade
    df_e = df_att.copy()
    e = (
        df_e
        .groupby('especialidade', dropna=False)
        .agg(
            atendimentos=('cod_atendimento', 'count'),
            atend_atb=('tem_antibiotico', 'sum'),
            soma_atb=('n_antibioticos', 'sum')
        )
        .reset_index()
    )
    e['pct_atend_atb'] = e['atend_atb'] / e['atendimentos']
    e['media_atb'] = e['soma_atb'] / e['atendimentos']
    e = e[e['atendimentos'] >= min_n].copy()

    if metric == '% atendimentos com ATB':
        ycol_u = 'pct_atend_atb'
        ycol_e = 'pct_atend_atb'
        ytitle = '% atendimentos com ATB'
        yfmt = '.0%'
    else:
        ycol_u = 'media_atb'
        ycol_e = 'media_atb'
        ytitle = 'Média de ATBs por atendimento'
        yfmt = None

    # rótulos curtos
    u['nome_unidade_label'] = u['nome_unidade'].astype(str).str.slice(0, 35)
    u.loc[u['nome_unidade'].astype(str).str.len() > 35, 'nome_unidade_label'] += '…'

    e['especialidade_label'] = e['especialidade'].astype(str).str.slice(0, 35)
    e.loc[e['especialidade'].astype(str).str.len() > 35, 'especialidade_label'] += '…'

    u = u.sort_values(ycol_u, ascending=False).head(top_n).sort_values(ycol_u, ascending=True)
    e = e.sort_values(ycol_e, ascending=False).head(top_n).sort_values(ycol_e, ascending=True)

    c1, c2 = st.columns(2)

    with c1:
        st.markdown('**UBS**')
        fig = px.bar(
            u,
            y='nome_unidade_label',
            x=ycol_u,
            orientation='h',
            hover_data={
                'nome_unidade': True,
                'nome_unidade_label': False,
                'atendimentos': True,
                'atend_atb': True,
                'pct_atend_atb': ':.1%' if ycol_u == 'pct_atend_atb' else False,
                'media_atb': ':.2f' if ycol_u == 'media_atb' else False,
            }
        )
        fig.update_xaxes(title=ytitle, tickformat=yfmt)
        fig.update_yaxes(title=None)
        fig.update_layout(height=520, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown('**Especialidade**')
        fig2 = px.bar(
            e,
            y='especialidade_label',
            x=ycol_e,
            orientation='h',
            hover_data={
                'especialidade': True,
                'especialidade_label': False,
                'atendimentos': True,
                'atend_atb': True,
                'pct_atend_atb': ':.1%' if ycol_e == 'pct_atend_atb' else False,
                'media_atb': ':.2f' if ycol_e == 'media_atb' else False,
            }
        )
        fig2.update_xaxes(title=ytitle, tickformat=yfmt)
        fig2.update_yaxes(title=None)
        fig2.update_layout(height=520, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig2, use_container_width=True)


# =============================================================================
# TAB 3: Medicamentos (ATB)
# =============================================================================
with tab3:
    st.subheader('Antibióticos mais prescritos (nível prescrição)')

    df_m = df_raw.copy()
    df_m['e_antibiotico'] = df_m['e_antibiotico'].fillna(0).astype(int)
    df_m = df_m[df_m['e_antibiotico'] == 1].copy()

    col1, col2 = st.columns(2)
    with col1:
        # group_med = st.selectbox(
        #     'Agrupar por',
        #     options=['composto_quimico', 'nome_medicamento'],
        #     index=0
        # )
        group_map = {
            'Composto químico (princípio ativo)': 'composto_quimico',
            'Medicamento (nome comercial)': 'nome_medicamento',
        }

        group_label = st.selectbox(
            'Agrupar por',
            list(group_map.keys()),
            index=0,  # default: composto_quimico
        )

        group_med = group_map[group_label]
    with col2:
        top_med = st.slider('Top N antibióticos', 5, 30, 15, 1)

    if df_m.empty:
        st.info('Nenhuma prescrição de antibiótico no recorte atual.')
    else:
        med = (
            df_m
            .groupby(group_med, dropna=False)
            .size()
            .reset_index(name='prescricoes')
            .sort_values('prescricoes', ascending=False)
            .head(top_med)
        )

        med[f'{group_med}_label'] = med[group_med].astype(str).str.slice(0, 45)
        med.loc[med[group_med].astype(str).str.len() > 45, f'{group_med}_label'] += '…'
        med = med.sort_values('prescricoes', ascending=True)

        fig = px.bar(
            med,
            y=f'{group_med}_label',
            x='prescricoes',
            orientation='h',
            hover_data={
                group_med: True,
                f'{group_med}_label': False,
                'prescricoes': True
            }
        )
        fig.update_xaxes(title='Prescrições')
        fig.update_yaxes(title=None)
        fig.update_layout(height=520, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.subheader('Tabela de detalhe (prescrições com ATB)')

    cols = [
        'cod_atendimento', 'data_atendimento', 'nome_unidade', 'especialidade',
        'cod_cid_ciap', 'diag_analise', 'composto_quimico', 'nome_medicamento',
        'concentracao', 'unidade_apresentacao', 'duracao'
    ]
    cols = [c for c in cols if c in df_m.columns]

    st.dataframe(
        df_m[cols].sort_values('data_atendimento', ascending=False).head(2000),
        use_container_width=True,
        height=420
    )
