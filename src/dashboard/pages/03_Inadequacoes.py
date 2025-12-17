# TODO: Ver BACKLOG.md — classificação de ATB fora de guideline

# src/dashboard/pages/03_Inadequacoes.py
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
    page_title="CAMO-NET",
    page_icon="🦠",
)

st.title('Inadequações: CID Infeccioso x Antibiótico')
st.caption(
    'Unidade de análise: atendimento (cod_atendimento). '
    'As flags são agregadas via OR lógico (max) a partir das prescrições.'
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


def _kpi_card(label: str, value, help_text: str | None = None):
    st.metric(label=label, value=value, help=help_text)


def _build_quadrant(df_att: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna DF com classificação por quadrante:
    - 'CID infeccioso + ATB' (esperado)
    - 'CID infeccioso sem ATB' (atenção)
    - 'ATB sem CID infeccioso' (atenção)
    - 'Sem CID infeccioso e sem ATB' (esperado)
    """
    tmp = df_att.copy()

    cid = tmp['tem_cid_infeccioso'].astype(int)
    atb = tmp['tem_antibiotico'].astype(int)

    def classify(c: int, a: int) -> str:
        if c == 1 and a == 1:
            return 'CID infeccioso + ATB'
        if c == 1 and a == 0:
            return 'CID infeccioso sem ATB'
        if c == 0 and a == 1:
            return 'ATB sem CID infeccioso'
        return 'Sem CID infeccioso e sem ATB'

    tmp['quadrante'] = [classify(c, a) for c, a in zip(cid, atb)]
    return tmp


# =============================================================================
# LOAD
# =============================================================================
with st.spinner('Carregando dados...'):
    df_raw, df_att_base = _load_data()

if df_raw.empty or df_att_base.empty:
    st.error('Não foi possível carregar os dados (DataFrame vazio).')
    st.stop()

# =============================================================================
# SIDEBAR FILTERS
# =============================================================================
with st.sidebar:
    st.subheader('Filtros')

    dt_min, dt_max = _safe_dt_range(df_att_base['data_atendimento'])

    st.markdown('Data Início:')
    d_start = st.date_input(
        ' ',
        value=st.session_state.get('flt_start', dt_min),
        min_value=dt_min,
        max_value=dt_max,
        label_visibility='collapsed',
        key='flt_start'
    )

    st.markdown('Data Fim:')
    d_end = st.date_input(
        ' ',
        value=st.session_state.get('flt_end', dt_max),
        min_value=dt_min,
        max_value=dt_max,
        label_visibility='collapsed',
        key='flt_end'
    )

    df_filtered = df_att_base[ (df_att_base['data_atendimento'].dt.date >= d_start) &
                    (df_att_base['data_atendimento'].dt.date <= d_end)].copy()


    # Opções categóricas
    unidades = sorted([x for x in df_filtered['nome_unidade'].dropna().unique().tolist()])
    especialidades = sorted([x for x in df_filtered['especialidade'].dropna().unique().tolist()])
    faixas = sorted([x for x in df_filtered['faixa_etaria'].dropna().unique().tolist()])

    sel_unidades = st.multiselect('Unidade de saúde', options=unidades, default=[])
    sel_especialidades = st.multiselect('Especialidade', options=especialidades, default=[])
    sel_faixas = st.multiselect('Faixa etária', options=faixas, default=[])

    # Sexo no nível atendimento
    sexo_map = {'m': 'Masculino', 'f': 'Feminino'}
    sexo_opts = ['Todos', 'Masculino', 'Feminino']
    sel_sexo = st.selectbox('Sexo', options=sexo_opts, index=0)

    st.divider()
    st.subheader('Foco')
    focus_opts = [
            'Todos os atendimentos',
            'Somente inconsistências (CID sem ATB ou ATB sem CID)',
            'Somente CID infeccioso sem ATB',
            'Somente ATB sem CID infeccioso',
            # 'Somente com prescrição inadequada',
        ]
    sel_focus = st.radio('Recorte principal', options=focus_opts, index=0)

# Aplica filtros (nível atendimento)
df_filtered = df_filtered.copy()

if sel_unidades:
    df_filtered = df_filtered[df_filtered['nome_unidade'].isin(sel_unidades)]

if sel_especialidades:
    df_filtered = df_filtered[df_filtered['especialidade'].isin(sel_especialidades)]

if sel_faixas:
    df_filtered = df_filtered[df_filtered['faixa_etaria'].isin(sel_faixas)]

if sel_sexo != 'Todos':
    target = 'm' if sel_sexo == 'Masculino' else 'f'
    df_filtered = df_filtered[df_filtered['sexo'].astype(str).str.lower() == target]

# Classificação de quadrantes e filtros por foco
df_att = _build_quadrant(df_filtered)
is_cid_sem_atb = (df_att['tem_cid_infeccioso'] == 1) & (df_att['tem_antibiotico'] == 0)
is_atb_sem_cid = (df_att['tem_cid_infeccioso'] == 0) & (df_att['tem_antibiotico'] == 1)
is_inconsistente = is_cid_sem_atb | is_atb_sem_cid
# is_inadequado = (df_att['tem_presc_inadequada'] == 1)

if sel_focus == 'Somente inconsistências (CID sem ATB ou ATB sem CID)':
    df_att = df_att[is_inconsistente]
elif sel_focus == 'Somente CID infeccioso sem ATB':
    df_att = df_att[is_cid_sem_atb]
elif sel_focus == 'Somente ATB sem CID infeccioso':
    df_att = df_att[is_atb_sem_cid]
# elif sel_focus == 'Somente com prescrição inadequada':
#     df_att = df_att[is_inadequado]


# =============================================================================
# KPIs
# =============================================================================
total_atend = int(df_att.shape[0])
total_infecc = int((df_att['tem_cid_infeccioso'] == 1).sum())
total_atb = int((df_att['tem_antibiotico'] == 1).sum())
total_inadequ = int((df_att['tem_presc_inadequada'] == 1).sum())

cid_sem_atb = int(((df_att['tem_cid_infeccioso'] == 1) & (df_att['tem_antibiotico'] == 0)).sum())
atb_sem_cid = int(((df_att['tem_cid_infeccioso'] == 0) & (df_att['tem_antibiotico'] == 1)).sum())

pct_inadequ = (total_inadequ / total_atend) if total_atend else 0.0
pct_cid_sem_atb = (cid_sem_atb / total_infecc) if total_infecc else 0.0
pct_atb_sem_cid = (atb_sem_cid / total_atb) if total_atb else 0.0


kpi1, kpi2, kpi3, kpi4, kpi5, kpi6 = st.columns(6)
with kpi1:
    _kpi_card('Atendimentos', f'{total_atend:,}'.replace(',', '.'))
with kpi2:
    _kpi_card('Com CID infeccioso', f'{total_infecc:,}'.replace(',', '.'))
with kpi3:
    _kpi_card('Com ATB', f'{total_atb:,}'.replace(',', '.'))
with kpi4:
    _kpi_card('Com inadequação', f'{total_inadequ:,}'.replace(',', '.'), help_text='Nível atendimento')
with kpi5:
    _kpi_card('CID infeccioso sem ATB', f'{cid_sem_atb:,}'.replace(',', '.'), help_text='Possível subtratamento')
with kpi6:
    _kpi_card('ATB sem CID infeccioso', f'{atb_sem_cid:,}'.replace(',', '.'), help_text='Possível uso indevido')

st.write(
    f'**Taxas no recorte atual** — '
    f'Inadequação: **{pct_inadequ:.1%}** | '
    f'CID infeccioso sem ATB: **{pct_cid_sem_atb:.1%}** | '
    f'ATB sem CID infeccioso: **{pct_atb_sem_cid:.1%}**'
)

st.divider()


# =============================================================================
# MAIN VISUALS
# =============================================================================
tab1, tab2 = st.tabs(['Visão Analítica', 'Inspeção'])

with tab1:
    c1, c2 = st.columns([1.05, 0.95])

    with c1:
        st.subheader('CID Infeccioso x Antibiótico (atendimentos)')

        # 2x2 pivot
        tmp = df_att.copy()

        tmp['tem_cid_infeccioso'] = tmp['tem_cid_infeccioso'].fillna(0).astype(int)
        tmp['tem_antibiotico'] = tmp['tem_antibiotico'].fillna(0).astype(int)

        tmp['CID infeccioso'] = tmp['tem_cid_infeccioso'].map({0: 'Não', 1: 'Sim'})
        tmp['Recebeu ATB'] = tmp['tem_antibiotico'].map({0: 'Não', 1: 'Sim'})

        mat = (
            tmp
            .groupby(['CID infeccioso', 'Recebeu ATB'], dropna=False)
            .size()
            .reset_index(name='atendimentos')
        )

        mat['CID infeccioso'] = pd.Categorical(mat['CID infeccioso'], categories=['Não', 'Sim'], ordered=True)
        mat['Recebeu ATB'] = pd.Categorical(mat['Recebeu ATB'], categories=['Não', 'Sim'], ordered=True)
        mat = mat.sort_values(['CID infeccioso', 'Recebeu ATB'])

        fig = px.density_heatmap(
            mat,
            x='Recebeu ATB',
            y='CID infeccioso',
            z='atendimentos',
            text_auto=True,
        )
        fig.update_layout(height=320, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig, use_container_width=True)

        st.caption(
            'Leitura recomendada: priorizar a investigação dos quadrantes '
            '"CID infeccioso sem ATB" e "ATB sem CID infeccioso".'
        )

    with c2:
        st.subheader('Distribuição por quadrante')

        quad = (
            df_att
            .groupby('quadrante')
            .size()
            .reset_index(name='atendimentos')
            .sort_values('atendimentos', ascending=False)
        )
        fig2 = px.bar(quad, x='quadrante', y='atendimentos')
        fig2.update_layout(height=320, margin=dict(l=20, r=20, t=40, b=20))
        fig2.update_xaxes(title=None)
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # Rankings por unidade/especialidade (taxas)
    left, right = st.columns(2)

    with left:
        st.subheader('UBS — taxa de inconsistências')

        df_unit = df_att.copy()
        df_unit['inconsistente'] = (
            ((df_unit['tem_cid_infeccioso'] == 1) & (df_unit['tem_antibiotico'] == 0)) |
            ((df_unit['tem_cid_infeccioso'] == 0) & (df_unit['tem_antibiotico'] == 1))
        ).astype(int)

        unit_stats = (
            df_unit
            .groupby('nome_unidade', dropna=False)
            .agg(
                atendimentos=('cod_atendimento', 'count'),
                inconsistentes=('inconsistente', 'sum'),
            )
            .reset_index()
        )

        unit_stats['taxa_inconsistencia'] = unit_stats['inconsistentes'] / unit_stats['atendimentos']
        unit_stats = unit_stats.sort_values('taxa_inconsistencia', ascending=False)

        # rótulo curto (hover mantém o nome completo)
        unit_stats['nome_unidade_label'] = unit_stats['nome_unidade'].astype(str).str.slice(0, 35)
        unit_stats.loc[unit_stats['nome_unidade'].astype(str).str.len() > 35, 'nome_unidade_label'] += '…'

        # TOP 10 + OUTROS
        top_n = 10
        top = unit_stats.head(top_n).copy()
        others = unit_stats.iloc[top_n:].copy()

        if not others.empty:
            others_row = pd.DataFrame([{
                'nome_unidade': 'Outros',
                'nome_unidade_label': 'Outros',
                'atendimentos': int(others['atendimentos'].sum()),
                'inconsistentes': int(others['inconsistentes'].sum()),
                'taxa_inconsistencia': (
                    others['inconsistentes'].sum() / others['atendimentos'].sum()
                    if others['atendimentos'].sum() > 0 else 0.0
                )
            }])
            top = pd.concat([top, others_row], ignore_index=True)

        # para ranking horizontal (melhor leitura)
        top = top.sort_values('taxa_inconsistencia', ascending=False)

        fig3 = px.bar(
            top,
            x='nome_unidade_label',
            y='taxa_inconsistencia',
            text='taxa_inconsistencia',
            hover_data=['atendimentos', 'inconsistentes']
        )
        fig3.update_traces(texttemplate='%{text:.1%}', textposition='outside', cliponaxis=False)
        fig3.update_layout(height=380, margin=dict(l=20, r=20, t=40, b=20))
        fig3.update_xaxes(title=None, tickangle=30)
        fig3.update_yaxes(title=None, tickformat='.1%')
        st.plotly_chart(fig3, use_container_width=True)



    with right:
        st.subheader('Especialidade — taxa de ATB sem CID infeccioso')

        df_spec = df_att.copy()
        df_spec['atb_sem_cid'] = (df_spec['tem_cid_infeccioso'] == 0) & (df_spec['tem_antibiotico'] == 1)

        spec_stats = (
            df_spec
            .groupby('especialidade', dropna=False)
            .agg(
                atendimentos=('cod_atendimento', 'count'),
                atb_sem_cid=('atb_sem_cid', 'sum')
            )
            .reset_index()
        )

        spec_stats['taxa_atb_sem_cid'] = spec_stats['atb_sem_cid'] / spec_stats['atendimentos']
        spec_stats = spec_stats.sort_values('taxa_atb_sem_cid', ascending=False)

        # encurtar rótulos (mantém hover completo)
        spec_stats['especialidade_label'] = spec_stats['especialidade'].astype(str).str.slice(0, 35)
        spec_stats.loc[spec_stats['especialidade'].astype(str).str.len() > 35, 'especialidade_label'] += '…'

        # agrupamento dos outros para exibir os 10+
        top = spec_stats.head(10).copy()
        others = spec_stats.iloc[10:].copy()
        if not others.empty:
            others_row = pd.DataFrame([{
                'especialidade': 'Outros',
                'especialidade_label': 'Outros',
                'atendimentos': int(others['atendimentos'].sum()),
                'atb_sem_cid': int(others['atb_sem_cid'].sum()),
                'taxa_atb_sem_cid': float(others['atb_sem_cid'].sum() / others['atendimentos'].sum())
            }])
            top = pd.concat([top, others_row], ignore_index=True)
        top = top.sort_values('taxa_atb_sem_cid', ascending=True)    

        fig4 = px.bar(
            top,
            x='especialidade_label',
            y='taxa_atb_sem_cid',
            text='taxa_atb_sem_cid',
            hover_data={
                'especialidade': True,
                'especialidade_label': False,
                'atendimentos': True,
                'atb_sem_cid': True,
                'taxa_atb_sem_cid': ':.1%'
            },
        )

        ymax = float(top['taxa_atb_sem_cid'].max())
        ymax = max(ymax, 0.01)  # evita zero
        fig4.update_yaxes(
            title='Taxa de ATB sem CID infeccioso',
            tickformat='.1%',
            range=[0, min(1.0, ymax * 1.2)],  # 20% de folga e nunca acima de 100%
        )
        fig4.update_traces(texttemplate='%{text:.1%}', textposition='outside', cliponaxis=False)
        fig4.update_xaxes(title=None)
        fig4.update_layout(height=360, margin=dict(l=20, r=20, t=40, b=20))
        fig4.update_layout(uniformtext_minsize=10, uniformtext_mode='hide')
        st.plotly_chart(fig4, use_container_width=True)

    st.divider()

    st.subheader('Evolução mensal')

    df_time = df_att.copy()
    df_time['ano_mes'] = df_time['data_atendimento'].dt.to_period('M').astype(str)
    df_time['inadequado'] = (df_time['tem_presc_inadequada'] == 1).astype(int)

    series = (
        df_time
        .groupby('ano_mes')
        .agg(
            atendimentos=('cod_atendimento', 'count'),
            inadequados=('inadequado', 'sum'),
        )
        .reset_index()
    )
    series['taxa_inadequacao'] = series['inadequados'] / series['atendimentos']

    fig5 = px.line(series, x='ano_mes', y='taxa_inadequacao', markers=True)
    fig5.update_layout(height=320, margin=dict(l=20, r=20, t=40, b=20))
    fig5.update_yaxes(title='Taxa de Inadequação', tickformat='.1%')
    st.plotly_chart(fig5, use_container_width=True)


with tab2:
    st.subheader('Lista de atendimentos no recorte atual')

    cols_show = [
        'cod_atendimento',
        'data_atendimento',
        'nome_unidade',
        'especialidade',
        'sexo',
        'idade',
        'faixa_etaria',
        'tem_cid_infeccioso',
        'tem_antibiotico',
        'tem_presc_inadequada',
        'n_prescricoes',
        'n_antibioticos',
        'quadrante',
    ]

    df_show = df_att[cols_show].copy()
    df_show = df_show.sort_values('data_atendimento', ascending=False)

    st.dataframe(df_show, use_container_width=True, height=420)

    st.divider()
    st.subheader('Detalhe de prescrições para um atendimento')

    chosen = st.selectbox(
        'Selecione um cod_atendimento para inspecionar as prescrições',
        options=df_show['cod_atendimento'].astype(str).head(5000).tolist(),
        index=0 if not df_show.empty else None
    )

    if chosen:
        cod = chosen
        df_detail = df_raw[df_raw['cod_atendimento'].astype(str) == cod].copy()
        df_detail = df_detail.sort_values('data_atendimento', ascending=False)

        cols_detail = [
            'cod_atendimento',
            'data_atendimento',
            'nome_unidade',
            'especialidade',
            'cod_cid_ciap',
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
        ]
        cols_detail = [c for c in cols_detail if c in df_detail.columns]

        st.dataframe(df_detail[cols_detail], use_container_width=True, height=360)
