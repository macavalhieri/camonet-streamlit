# src/dashboard/pages/01_Atendimentos.py
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

st.title('Atendimentos Infecciosos (contexto clínico)')
st.caption(
    'Unidade de análise: atendimento (cod_atendimento). '
    'Diagnóstico infeccioso e ATB são agregados a partir das prescrições.'
)


# =============================================================================
# HELPERS
# =============================================================================
@st.cache_data(show_spinner=False)
def _load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    df = load_gold_data()
    if df.empty:
        return df, df

    # defensivo: flags em 0/1
    for col in ['e_diag_infeccioso', 'e_antibiotico', 'e_presc_inadequada']:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)

    df['data_atendimento'] = pd.to_datetime(df['data_atendimento'], errors='coerce')

    df_att = build_attendance_level_df(df)
    df_att['data_atendimento'] = pd.to_datetime(df_att['data_atendimento'], errors='coerce')

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


def _truncate(s: pd.Series, max_len: int = 45) -> pd.Series:
    s = s.astype(str)
    out = s.str.slice(0, max_len)
    out[s.str.len() > max_len] = out[s.str.len() > max_len] + '…'
    return out


def _apply_filters(df_raw: pd.DataFrame, df_att: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    dt_min, dt_max = _safe_dt_range(df_att['data_atendimento'])
    min_d, max_d = dt_min.date(), dt_max.date()

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

        unidades = sorted([x for x in df_att['nome_unidade'].dropna().unique().tolist()])
        especialidades = sorted([x for x in df_att['especialidade'].dropna().unique().tolist()])
        faixas = sorted([x for x in df_att['faixa_etaria'].dropna().unique().tolist()])

        sel_unidades = st.multiselect('Unidade de saúde', options=unidades, default=[])
        sel_especialidades = st.multiselect('Especialidade', options=especialidades, default=[])
        sel_faixas = st.multiselect('Faixa etária', options=faixas, default=[])

        sexo_opts = ['Todos', 'Masculino', 'Feminino']
        sel_sexo = st.selectbox('Sexo', options=sexo_opts, index=0)

        st.divider()

    # Filtro base em atendimento
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

    # Filtra df_raw pelo conjunto de atendimentos final
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

min_n = st.session_state.get('min_n_rank', 50)
top_n = st.session_state.get('top_n_rank', 15)

# Subset infeccioso (sempre útil para KPIs)
df_inf = df_att[df_att['tem_cid_infeccioso'].fillna(0).astype(int) == 1].copy()


# =============================================================================
# KPIs
# =============================================================================
total_atend = int(df_att.shape[0])
total_inf = int(df_inf.shape[0])
pct_inf = (total_inf / total_atend) if total_atend else 0.0

inf_com_atb = int((df_inf['tem_antibiotico'].fillna(0).astype(int) == 1).sum())
inf_sem_atb = total_inf - inf_com_atb
pct_inf_com_atb = (inf_com_atb / total_inf) if total_inf else 0.0

k1, k2, k3, k4, k5, k6 = st.columns(6)
with k1:
    st.metric('Atendimentos', _format_int(total_atend))
with k2:
    st.metric('Atendimentos infecciosos', _format_int(total_inf))
with k3:
    st.metric('% infecciosos', _format_pct(pct_inf))
with k4:
    st.metric('Infecciosos com ATB', _format_int(inf_com_atb))
with k5:
    st.metric('Infecciosos sem ATB', _format_int(inf_sem_atb))
with k6:
    st.metric('% infecciosos com ATB', _format_pct(pct_inf_com_atb))

st.divider()


# =============================================================================
# TABS
# =============================================================================
tab1, tab2, tab3 = st.tabs(['Tendência', 'Distribuições', 'Inspeção'])


# =============================================================================
# TAB 1: Tendência
# =============================================================================
with tab1:
    tmp = df_att[df_att['data_atendimento'].notna()].copy()
    tmp['ano_mes'] = tmp['data_atendimento'].dt.to_period('M').astype(str)

    monthly = (
        tmp
        .groupby('ano_mes')
        .agg(
            atendimentos=('cod_atendimento', 'count'),
            infecciosos=('tem_cid_infeccioso', 'sum'),
            atend_atb=('tem_antibiotico', 'sum'),
        )
        .reset_index()
    )
    monthly['pct_infecciosos'] = monthly['infecciosos'] / monthly['atendimentos']
    monthly['pct_inf_com_atb'] = monthly.apply(
        lambda r: (r['atend_atb'] / r['infecciosos']) if r['infecciosos'] > 0 else 0.0,
        axis=1
    )

    c1, c2 = st.columns(2)
    with c1:
        st.subheader('Atendimentos Infecciosos')
        fig1 = px.line(monthly, x='ano_mes', y='pct_infecciosos', markers=True)
        fig1.update_yaxes(title='% atendimentos infecciosos', tickformat='.0%')
        fig1.update_xaxes(title=None)
        fig1.update_layout(height=340, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig1, use_container_width=True)

    with c2:
        st.subheader('Infecciosos com ATB')
        fig2 = px.line(monthly, x='ano_mes', y='pct_inf_com_atb', markers=True)
        fig2.update_yaxes(title='% infecciosos com ATB', tickformat='.0%')
        fig2.update_xaxes(title=None)
        fig2.update_layout(height=340, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()
    st.subheader('Volumes mensais')

    m_long = monthly.melt(
        id_vars=['ano_mes'],
        value_vars=['atendimentos', 'infecciosos', 'atend_atb'],
        var_name='serie',
        value_name='qtde'
    )
    fig3 = px.line(m_long, x='ano_mes', y='qtde', color='serie', markers=True)
    fig3.update_xaxes(title=None)
    fig3.update_yaxes(title='Quantidade')
    fig3.update_layout(height=340, margin=dict(l=20, r=20, t=40, b=20))
    st.plotly_chart(fig3, use_container_width=True)


# =============================================================================
# TAB 2: Distribuições
# =============================================================================
with tab2:
    if df_inf.empty:
        st.info('Nenhum atendimento infeccioso no recorte atual.')
    else:
        # UBS: % de infecciosos com ATB
        u = (
            df_inf
            .groupby('nome_unidade', dropna=False)
            .agg(
                atend_inf=('cod_atendimento', 'count'),
                inf_com_atb=('tem_antibiotico', 'sum')
            )
            .reset_index()
        )
        u['pct_inf_com_atb'] = u['inf_com_atb'] / u['atend_inf']
        u = u[u['atend_inf'] >= min_n].sort_values('pct_inf_com_atb', ascending=False).head(top_n)
        u['nome_unidade_label'] = _truncate(u['nome_unidade'], 35)
        u = u.sort_values('pct_inf_com_atb', ascending=True)

        # Especialidade: % de infecciosos com ATB
        e = (
            df_inf
            .groupby('especialidade', dropna=False)
            .agg(
                atend_inf=('cod_atendimento', 'count'),
                inf_com_atb=('tem_antibiotico', 'sum')
            )
            .reset_index()
        )
        e['pct_inf_com_atb'] = e['inf_com_atb'] / e['atend_inf']
        e = e[e['atend_inf'] >= min_n].sort_values('pct_inf_com_atb', ascending=False).head(top_n)
        e['especialidade_label'] = _truncate(e['especialidade'], 35)
        e = e.sort_values('pct_inf_com_atb', ascending=True)

        c1, c2 = st.columns(2)
        with c1:
            st.markdown('**UBS — % infecciosos com ATB**')
            fig_u = px.bar(
                u,
                y='nome_unidade_label',
                x='pct_inf_com_atb',
                orientation='h',
                hover_data={
                    'nome_unidade': True,
                    'nome_unidade_label': False,
                    'atend_inf': True,
                    'inf_com_atb': True,
                    'pct_inf_com_atb': ':.1%'
                }
            )
            fig_u.update_xaxes(title=None, tickformat='.0%')
            fig_u.update_yaxes(title=None)
            fig_u.update_layout(height=520, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_u, use_container_width=True)

        with c2:
            st.markdown('**Especialidade — % infecciosos com ATB**')
            fig_e = px.bar(
                e,
                y='especialidade_label',
                x='pct_inf_com_atb',
                orientation='h',
                hover_data={
                    'especialidade': True,
                    'especialidade_label': False,
                    'atend_inf': True,
                    'inf_com_atb': True,
                    'pct_inf_com_atb': ':.1%'
                }
            )
            fig_e.update_xaxes(title=None, tickformat='.0%')
            fig_e.update_yaxes(title=None)
            fig_e.update_layout(height=520, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_e, use_container_width=True)

    st.divider()
    st.markdown('**Top diagnósticos infecciosos (nível registro)**')

    dfx = df_raw.copy()
    dfx['e_diag_infeccioso'] = dfx['e_diag_infeccioso'].fillna(0).astype(int)
    dfx = dfx[dfx['e_diag_infeccioso'] == 1].copy()

    if dfx.empty:
        st.info('Nenhum registro com diagnóstico infeccioso no recorte atual.')
    else:
        colA, colB, colC = st.columns(3)

        with colA:
            # diag_dim = st.selectbox(
            #     'Dimensão',
            #     options=['diag_agrupado', 'diag_analise', 'cod_cid_ciap'],
            #     index=0
            # )
            dim_map = {
                'Diagnóstico agrupado': 'diag_agrupado',
                'Diagnóstico detalhado': 'diag_analise',
                'Código diagnóstico (CID/CIAP)': 'cod_cid_ciap'
            }

            dim_label = st.selectbox(
                'Dimensão',
                list(dim_map.keys())
            )

            diag_dim = dim_map[dim_label]

        with colB:
            top_diag = st.slider('Top N diagnósticos', 5, 30, 15, 1)

        with colC:
            excluir_nao_especificado = st.toggle('Excluir "Não especificado"', value=False)

        # -------------------------------------------------------------------------
        # Filtro opcional: remover "Não especificado" (e variações comuns)
        # -------------------------------------------------------------------------
        if excluir_nao_especificado:
            s = dfx[diag_dim].astype(str).str.strip().str.lower()

            mask_nao_especificado = (
                s.isin(['não especificado', 'nao especificado', 'não informado', 'nao informado', 'n/a', 'na', 'nan'])
                | s.str.fullmatch(r'(não|nao)\s*especificad[oa]', na=False)
            )

            dfx = dfx[~mask_nao_especificado].copy()

        diag = (
            dfx
            .groupby(diag_dim, dropna=False)
            .agg(
                atendimentos=('cod_atendimento', 'nunique'),
                registros=('cod_atendimento', 'count'),
            )
            .reset_index()
            .sort_values('atendimentos', ascending=False)
            .head(top_diag)
        )

        diag[f'{diag_dim}_label'] = _truncate(diag[diag_dim], 45)
        diag = diag.sort_values('atendimentos', ascending=True)

        fig_d = px.bar(
            diag,
            y=f'{diag_dim}_label',
            x='atendimentos',
            orientation='h',
            hover_data={
                diag_dim: True,
                f'{diag_dim}_label': False,
                'atendimentos': True,
                'registros': True
            }
        )
        fig_d.update_xaxes(title='Atendimentos')
        fig_d.update_yaxes(title=None)
        fig_d.update_layout(height=520, margin=dict(l=20, r=20, t=40, b=20))
        st.plotly_chart(fig_d, use_container_width=True)



# =============================================================================
# TAB 3: Drill-down
# =============================================================================
with tab3:
    st.subheader('Lista de atendimentos infecciosos')

    if df_inf.empty:
        st.info('Nenhum atendimento infeccioso no recorte atual.')
        st.stop()

    df_list = df_inf.sort_values('data_atendimento', ascending=False).copy()
    cols = [
        'cod_atendimento', 'data_atendimento', 'nome_unidade', 'especialidade',
        'sexo', 'idade', 'faixa_etaria', 'tem_antibiotico', 'n_antibioticos',
        'n_prescricoes'
    ]
    cols = [c for c in cols if c in df_list.columns]
    st.dataframe(df_list[cols].head(3000), use_container_width=True, height=420)

    st.divider()
    st.subheader('Detalhe de um atendimento')

    chosen = st.selectbox(
        'Selecione um cod_atendimento',
        options=df_list['cod_atendimento'].astype(str).head(5000).tolist(),
        index=0
    )

    det = df_raw[df_raw['cod_atendimento'].astype(str) == str(chosen)].copy()
    det = det.sort_values('data_atendimento', ascending=False)

    det_cols = [
        'cod_atendimento', 'data_atendimento', 'nome_unidade', 'especialidade',
        'cod_cid_ciap', 'diagnosticar_por', 'diag_agrupado', 'diag_analise',
        'e_diag_infeccioso',
        'cod_medicamento', 'nome_medicamento', 'composto_quimico',
        'concentracao', 'unidade_apresentacao', 'duracao',
        'e_antibiotico', 'e_presc_inadequada'
    ]
    det_cols = [c for c in det_cols if c in det.columns]
    st.dataframe(det[det_cols], use_container_width=True, height=380)
