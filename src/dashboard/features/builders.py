import pandas as pd


def build_attendance_level_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega o DataFrame de prescrições para o nível de atendimento (cod_atendimento).

    Cada linha do DataFrame resultante representa um atendimento único, com
    variáveis clínicas e métricas derivadas das prescrições associadas.

    Regras:
    - Flags clínicas são calculadas por OR lógico (max).
    - Contagens refletem o volume de prescrições por atendimento.

    Parâmetros
    ----------
    df : pd.DataFrame
        DataFrame no nível de prescrição.

    Retorna
    -------
    pd.DataFrame
        DataFrame agregado no nível de atendimento.
    """
    return (
        df
        .groupby('cod_atendimento', as_index=False)
        .agg(
            data_atendimento=('data_atendimento', 'min'),
            cod_paciente=('cod_paciente', 'first'),
            sexo=('sexo', 'first'),
            idade=('idade', 'first'),
            faixa_etaria=('faixa_etaria', 'first'),
            cod_unidade_saude=('cod_unidade_saude', 'first'),
            nome_unidade=('nome_unidade', 'first'),
            especialidade=('especialidade', 'first'),

            # flags clínicas
            tem_cid_infeccioso=('e_diag_infeccioso', 'max'),
            tem_antibiotico=('e_antibiotico', 'max'),
            tem_presc_inadequada=('e_presc_inadequada', 'max'),

            # métricas de volume
            n_prescricoes=('cod_medicamento', 'count'),
            n_antibioticos=('e_antibiotico', 'sum'),
        )
    )
