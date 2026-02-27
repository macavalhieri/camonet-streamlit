# src/dashboard/pages/04_Antimicrobiana.py
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

st.title('Análise Antimicrobiana — Diagnóstico infeccioso x Tratamento (nível registro)')
st.caption(
    'Unidade de análise primária do gráfico: registro (df_raw), filtrado a partir de atendimentos (df_att). '
    'O gráfico empilhado mostra a distribuição percentual de compostos antimicrobianos '
    'por diagnóstico infeccioso.'
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


def _truncate(s: pd.Series, max_len: int = 45) -> pd.Series:
    s = s.astype(str)
    out = s.str.slice(0, max_len)
    out[s.str.len() > max_len] = out[s.str.len() > max_len] + '…'
    return out


def _apply_filters(df_raw: pd.DataFrame, df_att: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
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
        st.subheader('Parâmetros do gráfico')

        # diag_dim = st.selectbox(
        #     'Dimensão do diagnóstico',
        #     options=['diag_agrupado', 'diag_analise', 'cod_cid_ciap'],
        #     index=0
        # )
        diag_dim_map = {
            'Diagnóstico (agrupado)': 'diag_agrupado',
            'Diagnóstico (análise)': 'diag_analise',
            'Código do diagnóstico (CID/CIAP)': 'cod_cid_ciap',
        }

        diag_dim_label = st.selectbox(
            'Dimensão do diagnóstico',
            list(diag_dim_map.keys()),
            index=0,  # default: diag_agrupado
        )

        diag_dim = diag_dim_map[diag_dim_label]

        # comp_dim = st.selectbox(
        #     'Dimensão do antibiótico',
        #     options=['composto_quimico', 'nome_medicamento', 'cod_medicamento'],
        #     index=0
        # )
        atb_dim_map = {
            'Composto químico (princípio ativo)': 'composto_quimico',
            'Medicamento (nome comercial)': 'nome_medicamento',
            'Código do medicamento': 'cod_medicamento',
        }

        atb_dim_label = st.selectbox(
            'Dimensão do antibiótico',
            list(atb_dim_map.keys()),
            index=0,  # default: composto_quimico
        )

        comp_dim = atb_dim_map[atb_dim_label]

        top_diag = st.slider('Top N diagnósticos (por atendimentos)', 5, 30, 15, 1)
        top_comp = st.slider('Top N compostos (legenda)', 5, 25, 12, 1)

        colA, colB = st.columns([0.55, 0.45])
        with colA:
            excluir_nao_especificado = st.toggle('Excluir "Não especificado"', value=False)
        with colB:
            apenas_diag_com_atb = st.toggle('Somente diag. infeccioso com ATB', value=True)

        st.caption(
            'Dica: aumente Top N diagnósticos com cautela para manter legibilidade.'
        )

    # Filtro base em atendimento
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

    params = {
        'diag_dim': diag_dim,
        'comp_dim': comp_dim,
        'top_diag': top_diag,
        'top_comp': top_comp,
        'excluir_nao_especificado': excluir_nao_especificado,
        'apenas_diag_com_atb': apenas_diag_com_atb,
        'd_start': d_start,
        'd_end': d_end,
    }
    return df_raw_f, df_att_f, params


def _is_nao_especificado(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip().str.lower()
    return (
        s.isin(['não especificado', 'nao especificado', 'não informado', 'nao informado', 'n/a', 'na', 'nan', ''])
        | s.str.fullmatch(r'(não|nao)\s*especificad[oa]', na=False)
    )


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
comp_dim = params['comp_dim']
top_diag = params['top_diag']
top_comp = params['top_comp']
excluir_nao_especificado = params['excluir_nao_especificado']
apenas_diag_com_atb = params['apenas_diag_com_atb']

# Subset infeccioso (nível atendimento) — útil para KPIs e recortes
df_inf_att = df_att[df_att['tem_cid_infeccioso'].fillna(0).astype(int) == 1].copy()

# =============================================================================
# KPIs
# =============================================================================
total_atend = int(df_att.shape[0])
total_inf = int(df_inf_att.shape[0])
pct_inf = (total_inf / total_atend) if total_atend else 0.0

total_presc = int(pd.to_numeric(df_att.get('n_prescricoes', 0), errors='coerce').fillna(0).sum())
total_atb_att = int((df_att['tem_antibiotico'].fillna(0).astype(int) == 1).sum())
inf_com_atb = int(((df_att['tem_cid_infeccioso'].fillna(0).astype(int) == 1) &
                   (df_att['tem_antibiotico'].fillna(0).astype(int) == 1)).sum())
pct_inf_com_atb = (inf_com_atb / total_inf) if total_inf else 0.0

k1, k2, k3, k4, k5, k6 = st.columns(6)
with k1:
    st.metric('Atendimentos', _format_int(total_atend))
with k2:
    st.metric('Atendimentos infecciosos', _format_int(total_inf))
with k3:
    st.metric('% infecciosos', _format_pct(pct_inf))
with k4:
    st.metric('Atendimentos com ATB', _format_int(total_atb_att))
with k5:
    st.metric('Infecciosos com ATB', _format_int(inf_com_atb))
with k6:
    st.metric('% inf. com ATB', _format_pct(pct_inf_com_atb))

st.divider()

# =============================================================================
# MAIN
# =============================================================================
tab1, tab2 = st.tabs(['Visão Analítica', 'Inspeção'])

with tab1:
    st.subheader('Tratamento por diagnóstico infeccioso (barras empilhadas 100%)')

    dfx = df_raw.copy()

    # Normalizações mínimas
    dfx['e_diag_infeccioso'] = pd.to_numeric(dfx.get('e_diag_infeccioso', 0), errors='coerce').fillna(0).astype(int)
    dfx['e_antibiotico'] = pd.to_numeric(dfx.get('e_antibiotico', 0), errors='coerce').fillna(0).astype(int)

    # Foco do gráfico: diagnóstico infeccioso
    dfx = dfx[dfx['e_diag_infeccioso'] == 1].copy()

    # Opcional: manter somente registros que são antibiótico (para “tratamento” estrito)
    if apenas_diag_com_atb:
        dfx = dfx[dfx['e_antibiotico'] == 1].copy()

    if dfx.empty:
        st.info('Nenhum registro infeccioso (ou com ATB, conforme filtro) no recorte atual.')
    else:
        # Filtro opcional: remover "Não especificado" do diagnóstico
        if excluir_nao_especificado:
            dfx = dfx[~_is_nao_especificado(dfx[diag_dim])].copy()

        # Seleciona TOP diagnósticos com base em nº de atendimentos (não registros)
        top_diag_df = (
            dfx.groupby(diag_dim, dropna=False)
               .agg(atendimentos=('cod_atendimento', 'nunique'))
               .reset_index()
               .sort_values('atendimentos', ascending=False)
               .head(top_diag)
        )
        top_diag_vals = top_diag_df[diag_dim].tolist()

        dfx = dfx[dfx[diag_dim].isin(top_diag_vals)].copy()

        if dfx.empty:
            st.info('Sem dados após aplicar o TOP diagnósticos e filtros.')
        else:
            # Agrupa compostos: mantém TOP N compostos na legenda; demais viram "Outros"
            comp_rank = (
                dfx.groupby(comp_dim, dropna=False)
                   .agg(atendimentos=('cod_atendimento', 'nunique'))
                   .reset_index()
                   .sort_values('atendimentos', ascending=False)
            )
            keep_comp = comp_rank.head(top_comp)[comp_dim].tolist()

            dfx['comp_plot'] = dfx[comp_dim].where(dfx[comp_dim].isin(keep_comp), other='Outros')

            # Base de contagem (atendimentos únicos por diagnóstico x composto)
            mat = (
                dfx.groupby([diag_dim, 'comp_plot'], dropna=False)
                   .agg(atendimentos=('cod_atendimento', 'nunique'))
                   .reset_index()
            )

            # Percentual por diagnóstico
            totals = (
                mat.groupby(diag_dim, as_index=False)
                   .agg(total=('atendimentos', 'sum'))
            )
            mat = mat.merge(totals, on=diag_dim, how='left')
            mat['pct'] = mat.apply(
                lambda r: (100.0 * r['atendimentos'] / r['total']) if r['total'] else 0.0,
                axis=1
            )

            # Labels truncados para o eixo X (diagnóstico)
            mat[f'{diag_dim}_label'] = _truncate(mat[diag_dim], 40)

            # Ordena diagnósticos por volume (desc) no eixo
            order_diag = (
                top_diag_df.assign(_label=_truncate(top_diag_df[diag_dim], 40))
                          .sort_values('atendimentos', ascending=False)['_label']
                          .tolist()
            )

            # Ordena compostos (mantém “Outros” no fim)
            comp_order = [c for c in keep_comp if c != 'Outros'] + (['Outros'] if 'Outros' in mat['comp_plot'].unique() else [])

            fig = px.bar(
                mat,
                x=f'{diag_dim}_label',
                y='pct',
                color='comp_plot',
                category_orders={
                    f'{diag_dim}_label': order_diag,
                    'comp_plot': comp_order
                },
                hover_data={
                    diag_dim: True,
                    f'{diag_dim}_label': False,
                    'comp_plot': True,
                    'pct': ':.1f',
                    'atendimentos': True,
                    'total': True,
                },
            )

            fig.update_layout(
                barmode='stack',
                height=520,
                margin=dict(l=20, r=20, t=40, b=20),
                legend_title_text='Antimicrobial chemical compounds',
            )
            fig.update_yaxes(title='Percentage', range=[0, 100])
            fig.update_xaxes(title=None, tickangle=45)

            st.plotly_chart(fig, use_container_width=True)

            st.caption(
                'Leitura: cada barra representa um diagnóstico infeccioso; a composição indica a fração (%) '
                'de atendimentos em que cada composto foi utilizado (top compostos + “Outros”).'
            )

with tab2:
    st.subheader('Inspeção de registros (df_raw) no recorte atual')

    st.caption(
        'Aqui o nível é registro (prescrição/linha). Use para validar por que um composto aparece em um diagnóstico.'
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
