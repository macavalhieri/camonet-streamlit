#!/usr/bin/env python3
"""
CAMO-Net ETL Pipeline - Silver to Gold Layer
=============================================
Cria modelo dimensional (star schema) otimizado para análises AMR.

Componentes:
- 6 Dimensões: atendimento, paciente, medicamento, diagnostico, tempo, unidade_saude
- 3 Fatos: fato_prescricao, fato_diagnostico, fato_atendimento_resumo

Autor: Engenheiro de Dados Sênior
Data: 2025-11-26
Baseado em: Maita et al. BMC Medical Informatics and Decision Making (2025)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import sys


# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def calcular_faixa_etaria(idade):
    """
    Calcula faixa etária baseada na idade.
    
    Args:
        idade: Idade em anos
        
    Returns:
        String com faixa etária
    """
    if pd.isna(idade):
        return 'Não informado'
    
    idade = int(idade)
    
    if idade < 1:
        return '0-1 ano'
    elif idade < 12:
        return '1-11 anos'
    elif idade < 18:
        return '12-17 anos'
    elif idade < 60:
        return '18-59 anos'
    else:
        return '60+ anos'


def classificar_who_aware(composto_quimico):
    """
    Classifica antibiótico segundo WHO AWaRe.
    
    NOTA: Esta é uma classificação simplificada.
    Para produção, usar tabela de referência oficial da OMS.
    
    Args:
        composto_quimico: Nome do composto químico
        
    Returns:
        Classificação WHO AWaRe
    """
    if pd.isna(composto_quimico):
        return 'Not Applicable'
    
    composto = str(composto_quimico).upper()
    
    # Access: Antibióticos de primeira linha
    access_list = [
        'AMOXICILINA', 'AMPICILINA', 'PENICILINA', 'DOXICICLINA',
        'CEFALEXINA', 'SULFAMETOXAZOL', 'TRIMETOPRIMA', 'METRONIDAZOL',
        'NITROFURANTOINA', 'GENTAMICINA'
    ]
    
    # Watch: Antibióticos de segunda linha (maior risco de resistência)
    watch_list = [
        'CIPROFLOXACINO', 'LEVOFLOXACINO', 'AZITROMICINA', 'CLARITROMICINA',
        'CEFTRIAXONA', 'CEFOTAXIMA', 'CEFUROXIMA', 'AMOXICILINA + CLAVULANATO'
    ]
    
    # Reserve: Antibióticos de última linha
    reserve_list = [
        'MEROPENEM', 'IMIPENEM', 'VANCOMICINA', 'LINEZOLIDA',
        'COLISTINA', 'TIGECICLINA', 'DAPTOMICINA'
    ]
    
    # Verificar em qual lista está
    for antibiotico in access_list:
        if antibiotico in composto:
            return 'Access'
    
    for antibiotico in watch_list:
        if antibiotico in composto:
            return 'Watch'
    
    for antibiotico in reserve_list:
        if antibiotico in composto:
            return 'Reserve'
    
    return 'Not Classified'


def classificar_espectro_acao(composto_quimico, tipo_uso):
    """
    Classifica espectro de ação do antibiótico.
    
    Args:
        composto_quimico: Nome do composto
        tipo_uso: Tipo de uso do medicamento
        
    Returns:
        Espectro de ação
    """
    if pd.isna(composto_quimico):
        return 'Não aplicável'
    
    composto = str(composto_quimico).upper()
    
    # Amplo espectro
    amplo_espectro = [
        'AMOXICILINA + CLAVULANATO', 'CIPROFLOXACINO', 'LEVOFLOXACINO',
        'CEFTRIAXONA', 'AZITROMICINA', 'MEROPENEM', 'IMIPENEM'
    ]
    
    # Espectro estreito
    estreito_espectro = [
        'PENICILINA', 'AMOXICILINA', 'CEFALEXINA', 'ERITROMICINA',
        'VANCOMICINA', 'METRONIDAZOL'
    ]
    
    for antibiotico in amplo_espectro:
        if antibiotico in composto:
            return 'Amplo'
    
    for antibiotico in estreito_espectro:
        if antibiotico in composto:
            return 'Estreito'
    
    return 'Específico'


# ============================================================================
# CRIAÇÃO DE DIMENSÕES
# ============================================================================

def criar_dim_tempo(silver_path, gold_path):
    """
    Cria dimensão tempo com calendário completo.
    """
    print("\n[1/6] Criando dim_tempo...")
    
    # Ler tabelas com datas
    # Usar TAB_ATENDIMENTO_ANALISE como fonte primária de datas
    atend = pd.read_parquet(silver_path / 'TAB_ATENDIMENTO_ANALISE.parquet')
    
    # Extrair datas únicas
    atend['data_atendimento'] = pd.to_datetime(atend['data_atendimento'], errors='coerce')
    datas_unicas = atend['data_atendimento'].dropna().unique()
    
    # Criar DataFrame de tempo
    dim_tempo = pd.DataFrame({
        'data_completa': pd.to_datetime(datas_unicas)
    }).sort_values('data_completa').reset_index(drop=True)
    
    # Adicionar surrogate key
    dim_tempo['sk_tempo'] = range(1, len(dim_tempo) + 1)
    
    # Extrair componentes de data
    dim_tempo['ano'] = dim_tempo['data_completa'].dt.year
    dim_tempo['mes'] = dim_tempo['data_completa'].dt.month
    dim_tempo['trimestre'] = dim_tempo['data_completa'].dt.quarter
    dim_tempo['semestre'] = dim_tempo['data_completa'].dt.month.apply(lambda x: 1 if x <= 6 else 2)
    dim_tempo['dia_semana'] = dim_tempo['data_completa'].dt.dayofweek
    dim_tempo['nome_mes'] = dim_tempo['data_completa'].dt.month_name()
    dim_tempo['ano_mes'] = dim_tempo['data_completa'].dt.to_period('M').astype(str)
    
    # Reordenar colunas
    dim_tempo = dim_tempo[[
        'sk_tempo', 'data_completa', 'ano', 'mes', 'trimestre',
        'semestre', 'dia_semana', 'nome_mes', 'ano_mes'
    ]]
    
    # Salvar
    output_file = gold_path / 'dim_tempo.parquet'
    dim_tempo.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
    
    print(f"  ✓ dim_tempo criada: {len(dim_tempo):,} registros")
    return dim_tempo


def criar_dim_unidade_saude(silver_path, gold_path):
    """
    Cria dimensão unidade de saúde.
    """
    print("\n[2/6] Criando dim_unidade_saude...")
    
    # Ler tabela
    unidades = pd.read_parquet(silver_path / 'TAB_UNIDADE_SAUDE.parquet')
    
    # Criar dimensão
    dim_unidade = unidades[[
        'cod_unidade_saude', 'tipo', 'e_analizada'
    ]].drop_duplicates().reset_index(drop=True)
    
    # Adicionar surrogate key
    dim_unidade['sk_unidade_saude'] = range(1, len(dim_unidade) + 1)
    
    # Reordenar
    dim_unidade = dim_unidade[[
        'sk_unidade_saude', 'cod_unidade_saude', 'tipo', 'e_analizada'
    ]]
    
    # Salvar
    output_file = gold_path / 'dim_unidade_saude.parquet'
    dim_unidade.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
    
    print(f"  ✓ dim_unidade_saude criada: {len(dim_unidade):,} registros")
    return dim_unidade


def criar_dim_atendimento(silver_path, gold_path):
    """
    Cria dimensão atendimento.
    """
    print("\n[3/6] Criando dim_atendimento...")
    
    # Ler tabela
    # Usar TAB_ATENDIMENTO_ANALISE
    atend = pd.read_parquet(silver_path / 'TAB_ATENDIMENTO_ANALISE.parquet')
    
    # Criar dimensão (remover duplicatas pois ANALISE tem 1 linha por diagnóstico)
    dim_atend = atend[[
        'cod_atendimento', 'especialidade', 'periodo_extracao'
    ]].drop_duplicates().reset_index(drop=True)
    
    # Adicionar surrogate key
    dim_atend['sk_atendimento'] = range(1, len(dim_atend) + 1)
    
    # Reordenar
    dim_atend = dim_atend[[
        'sk_atendimento', 'cod_atendimento', 'especialidade', 'periodo_extracao'
    ]]
    
    # Salvar
    output_file = gold_path / 'dim_atendimento.parquet'
    dim_atend.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
    
    print(f"  ✓ dim_atendimento criada: {len(dim_atend):,} registros")
    return dim_atend


def criar_dim_paciente(silver_path, gold_path):
    """
    Cria dimensão paciente.
    """
    print("\n[4/6] Criando dim_paciente...")
    
    # Ler tabela com informações de pacientes
    atend_analise = pd.read_parquet(silver_path / 'TAB_ATENDIMENTO_ANALISE.parquet')
    
    # Agregar para pegar informações únicas por paciente
    # (usar moda para sexo e média para idade)
    dim_pac = atend_analise.groupby('cod_paciente').agg({
        'sexo': lambda x: x.mode()[0] if len(x.mode()) > 0 else None,
        'idade': 'mean'
    }).reset_index()
    
    # Calcular faixa etária
    dim_pac['faixa_etaria'] = dim_pac['idade'].apply(calcular_faixa_etaria)
    dim_pac['idade_anos'] = dim_pac['idade'].round().astype('Int64')
    
    # Adicionar surrogate key
    dim_pac['sk_paciente'] = range(1, len(dim_pac) + 1)
    
    # Reordenar
    dim_pac = dim_pac[[
        'sk_paciente', 'cod_paciente', 'sexo', 'faixa_etaria', 'idade_anos'
    ]]
    
    # Salvar
    output_file = gold_path / 'dim_paciente.parquet'
    dim_pac.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
    
    print(f"  ✓ dim_paciente criada: {len(dim_pac):,} registros")
    return dim_pac


def criar_dim_medicamento(silver_path, gold_path):
    """
    Cria dimensão medicamento com classificações AMR.
    """
    print("\n[5/6] Criando dim_medicamento...")
    
    # Ler tabela
    med = pd.read_parquet(silver_path / 'TAB_MEDICAMENTO.parquet')
    
    # Selecionar colunas relevantes
    dim_med = med[[
        'cod_medicamento', 'composto_quimico', 'tipo_uso',
        'unidade_apresentacao', 'concentracao', 'e_antibiotico'
    ]].drop_duplicates().reset_index(drop=True)
    
    # Adicionar classificações AMR
    dim_med['classe_who_aware'] = dim_med['composto_quimico'].apply(classificar_who_aware)
    dim_med['espectro_acao'] = dim_med.apply(
        lambda row: classificar_espectro_acao(row['composto_quimico'], row['tipo_uso']),
        axis=1
    )
    
    # Via de administração (simplificado - em produção, usar dados reais)
    dim_med['via_administracao'] = 'Oral'  # Placeholder
    
    # Adicionar surrogate key
    dim_med['sk_medicamento'] = range(1, len(dim_med) + 1)
    
    # Reordenar
    dim_med = dim_med[[
        'sk_medicamento', 'cod_medicamento', 'composto_quimico', 'tipo_uso',
        'unidade_apresentacao', 'concentracao', 'e_antibiotico',
        'classe_who_aware', 'espectro_acao', 'via_administracao'
    ]]
    
    # Salvar
    output_file = gold_path / 'dim_medicamento.parquet'
    dim_med.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
    
    print(f"  ✓ dim_medicamento criada: {len(dim_med):,} registros")
    print(f"    - Antibióticos: {dim_med['e_antibiotico'].sum():,}")
    print(f"    - WHO Access: {(dim_med['classe_who_aware'] == 'Access').sum():,}")
    print(f"    - WHO Watch: {(dim_med['classe_who_aware'] == 'Watch').sum():,}")
    print(f"    - WHO Reserve: {(dim_med['classe_who_aware'] == 'Reserve').sum():,}")
    
    return dim_med


def criar_dim_diagnostico(silver_path, gold_path):
    """
    Cria dimensão diagnóstico consolidando CID e CIAP.
    """
    print("\n[6/6] Criando dim_diagnostico...")
    
    # Ler tabelas
    cid = pd.read_parquet(silver_path / 'TAB_CID_DIAGNOSTICO.parquet')
    ciap = pd.read_parquet(silver_path / 'TAB_CIAP_DIAGNOSTICO.parquet')
    
    # Processar CID
    dim_cid = cid[[
        'cod_cid', 'diag_original', 'diag_agrupado', 'diag_analise', 'e_infeccao'
    ]].copy()
    dim_cid['cod_ciap'] = None
    dim_cid['tipo_diagnostico'] = 'CID'
    dim_cid = dim_cid.rename(columns={'cod_cid': 'codigo_diagnostico'})
    
    # Processar CIAP
    dim_ciap = ciap[[
        'cod_ciap', 'diag_original', 'diag_agrupado', 'diag_analise', 'e_infeccao'
    ]].copy()
    dim_ciap['cod_cid'] = None
    dim_ciap['tipo_diagnostico'] = 'CIAP'
    dim_ciap = dim_ciap.rename(columns={'cod_ciap': 'codigo_diagnostico'})
    
    # Consolidar
    dim_diag = pd.concat([dim_cid, dim_ciap], ignore_index=True)
    
    # Remover duplicatas
    dim_diag = dim_diag.drop_duplicates(subset=['codigo_diagnostico']).reset_index(drop=True)
    
    # Adicionar surrogate key
    dim_diag['sk_diagnostico'] = range(1, len(dim_diag) + 1)
    
    # Reordenar
    dim_diag = dim_diag[[
        'sk_diagnostico', 'codigo_diagnostico', 'diag_original',
        'diag_agrupado', 'diag_analise', 'e_infeccao', 'tipo_diagnostico'
    ]]
    
    # Salvar
    output_file = gold_path / 'dim_diagnostico.parquet'
    dim_diag.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
    
    print(f"  ✓ dim_diagnostico criada: {len(dim_diag):,} registros")
    print(f"    - CID: {(dim_diag['tipo_diagnostico'] == 'CID').sum():,}")
    print(f"    - CIAP: {(dim_diag['tipo_diagnostico'] == 'CIAP').sum():,}")
    print(f"    - Infecciosos: {dim_diag['e_infeccao'].sum():,}")
    
    return dim_diag


# ============================================================================
# CRIAÇÃO DE FATOS
# ============================================================================

def criar_fato_prescricao(silver_path, gold_path, dimensoes):
    """
    Cria tabela fato de prescrições.
    """
    print("\n[FATO 1/3] Criando fato_prescricao...")
    
    # Desempacotar dimensões
    dim_tempo, dim_unidade, dim_atend, dim_pac, dim_med, dim_diag = dimensoes
    
    # Ler tabelas silver
    med_prescrito = pd.read_parquet(silver_path / 'TAB_MED_PRESCRITO.parquet')
    med_analise = pd.read_parquet(silver_path / 'TAB_MEDPRESCRITO_ANALISE.parquet')
    atend = pd.read_parquet(silver_path / 'TAB_ATENDIMENTO.parquet')
    atend_analise = pd.read_parquet(silver_path / 'TAB_ATENDIMENTO_ANALISE.parquet')
    
    # Base: med_analise (prescrições com análise de antibióticos)
    fato = med_analise.copy()
    
    # Enriquecer com dados de TAB_MED_PRESCRITO
    med_prescrito_cols = med_prescrito[['cod_atendimento', 'cod_medicamento', 'quantidade', 'qtd_receita']].copy()
    fato = fato.merge(
        med_prescrito_cols,
        on=['cod_atendimento', 'cod_medicamento'],
        how='left',
        suffixes=('', '_med_prescrito')
    )
    
    # Usar duracao de med_analise (já está no fato)
    # quantidade e qtd_receita vêm de med_prescrito
    
    # Join com atendimento para obter contexto
    # Usar TAB_ATENDIMENTO_ANALISE para data e paciente
    # Usar TAB_ATENDIMENTO apenas para unidade de saúde
    
    # Preparar dados de atendimento (ANALISE) - remover duplicatas de atendimento
    atend_info = atend_analise[['cod_atendimento', 'cod_paciente', 'data_atendimento']].drop_duplicates()
    atend_info['data_atendimento'] = pd.to_datetime(atend_info['data_atendimento'], errors='coerce')
    
    # Preparar dados de unidade (TAB_ATENDIMENTO)
    unidade_info = atend[['cod_atendimento', 'cod_unidade_saude']].drop_duplicates()
    
    # Merge info atendimento
    fato = fato.merge(
        atend_info,
        on='cod_atendimento',
        how='left'
    )
    
    # Merge info unidade
    fato = fato.merge(
        unidade_info,
        on='cod_atendimento',
        how='left'
    )
    
    # Join com atendimento_analise para obter diagnóstico
    # LÓGICA ATUALIZADA:
    # 1. Identificar se o atendimento teve ALGUM diagnóstico infeccioso (max)
    # 2. Se teve, priorizar o código do diagnóstico infeccioso para o link. Se não, usar o primeiro.
    
    # Agregação para saber se é infeccioso
    atend_flags = atend_analise.groupby('cod_atendimento')['e_diag_infeccioso'].max().rename('e_diag_infeccioso_agg').reset_index()
    
    # Obter o código do diagnóstico principal (priorizando infecciosos)
    # Ordenar por e_diag_infeccioso (desc) para que infecciosos fiquem primeiro
    atend_analise_sorted = atend_analise.sort_values(['cod_atendimento', 'e_diag_infeccioso'], ascending=[True, False])
    diag_principal = atend_analise_sorted.groupby('cod_atendimento').first().reset_index()
    
    # Merge das flags agregadas
    fato = fato.merge(
        atend_flags[['cod_atendimento', 'e_diag_infeccioso_agg']],
        on='cod_atendimento',
        how='left'
    )
    
    # Merge do código de diagnóstico (priorizado)
    fato = fato.merge(
        diag_principal[['cod_atendimento', 'cod_cid_ciap']],
        on='cod_atendimento',
        how='left'
    )
    
    # Atualizar a coluna e_diag_infeccioso para usar a agregada
    fato['e_diag_infeccioso'] = fato['e_diag_infeccioso_agg']
    fato = fato.drop(columns=['e_diag_infeccioso_agg'])
    
    # Substituir business keys por surrogate keys
    # Tempo
    fato = fato.merge(
        dim_tempo[['sk_tempo', 'data_completa']],
        left_on='data_atendimento',
        right_on='data_completa',
        how='left'
    ).drop(columns=['data_completa'])
    
    # Unidade
    fato = fato.merge(
        dim_unidade[['sk_unidade_saude', 'cod_unidade_saude']],
        on='cod_unidade_saude',
        how='left'
    )
    
    # Atendimento
    fato = fato.merge(
        dim_atend[['sk_atendimento', 'cod_atendimento']],
        on='cod_atendimento',
        how='left'
    )
    
    # Paciente
    fato = fato.merge(
        dim_pac[['sk_paciente', 'cod_paciente']],
        on='cod_paciente',
        how='left'
    )
    
    # Medicamento
    fato = fato.merge(
        dim_med[['sk_medicamento', 'cod_medicamento', 'tipo_uso', 'espectro_acao', 'classe_who_aware']],
        on='cod_medicamento',
        how='left'
    )
    
    # Diagnóstico (usar cod_cid_ciap como codigo_diagnostico)
    fato = fato.merge(
        dim_diag[['sk_diagnostico', 'codigo_diagnostico']],
        left_on='cod_cid_ciap',
        right_on='codigo_diagnostico',
        how='left'
    ).drop(columns=['codigo_diagnostico'])
    
    # Criar flags AMR
    fato['e_diagnostico_infeccioso'] = fato['e_diag_infeccioso'].fillna(False)
    # Prescrição apropriada: antibiótico para infecção
    # Prescrição inadequada: antibiótico para não-infecção
    fato['e_prescricao_apropriada'] = (fato['e_antibiotico'] == True) & (fato['e_diagnostico_infeccioso'] == True)
    fato['e_prescricao_inadequada'] = (fato['e_antibiotico'] == True) & (fato['e_diagnostico_infeccioso'] == False)
    
    # Adicionar surrogate key
    fato['sk_prescricao'] = range(1, len(fato) + 1)
    
    # Selecionar e reordenar colunas finais
    fato_final = fato[[
        'sk_prescricao',
        'sk_atendimento',
        'sk_paciente',
        'sk_medicamento',
        'sk_tempo',
        'sk_unidade_saude',
        'quantidade',
        'qtd_receita',
        'duracao',
        'concentracao',
        'e_antibiotico',
        'e_diagnostico_infeccioso',
        'e_prescricao_apropriada',
        'e_prescricao_inadequada',
        'tipo_uso',
        'espectro_acao',
        'classe_who_aware'
    ]].copy()
    
    # Salvar
    output_file = gold_path / 'fato_prescricao.parquet'
    fato_final.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
    
    print(f"  ✓ fato_prescricao criada: {len(fato_final):,} registros")
    print(f"    - Antibióticos: {fato_final['e_antibiotico'].sum():,}")
    print(f"    - Prescrições apropriadas: {fato_final['e_prescricao_apropriada'].sum():,}")
    print(f"    - Taxa de adequação: {(fato_final['e_prescricao_apropriada'].sum() / len(fato_final) * 100):.2f}%")
    
    return fato_final


def criar_fato_diagnostico(silver_path, gold_path, dimensoes):
    """
    Cria tabela fato de diagnósticos.
    """
    print("\n[FATO 2/3] Criando fato_diagnostico...")
    
    # Desempacotar dimensões
    dim_tempo, dim_unidade, dim_atend, dim_pac, dim_med, dim_diag = dimensoes
    
    # Ler tabelas
    atend_analise = pd.read_parquet(silver_path / 'TAB_ATENDIMENTO_ANALISE.parquet')
    atend = pd.read_parquet(silver_path / 'TAB_ATENDIMENTO.parquet')
    
    # Base: atend_analise (1 linha = 1 diagnóstico)
    fato = atend_analise.copy()
    
    # Converter data_atendimento para datetime (já existe na tabela)
    fato['data_atendimento'] = pd.to_datetime(fato['data_atendimento'], errors='coerce')
    
    # Obter cod_unidade_saude de TAB_ATENDIMENTO
    atend_temp = atend[['cod_atendimento', 'cod_unidade_saude']].copy()
    fato = fato.merge(
        atend_temp,
        on='cod_atendimento',
        how='left'
    )
    
    # Substituir business keys por surrogate keys
    fato = fato.merge(dim_tempo[['sk_tempo', 'data_completa']], left_on='data_atendimento', right_on='data_completa', how='left').drop(columns=['data_completa'])
    fato = fato.merge(dim_unidade[['sk_unidade_saude', 'cod_unidade_saude']], on='cod_unidade_saude', how='left')
    fato = fato.merge(dim_atend[['sk_atendimento', 'cod_atendimento']], on='cod_atendimento', how='left')
    fato = fato.merge(dim_pac[['sk_paciente', 'cod_paciente']], on='cod_paciente', how='left')
    fato = fato.merge(dim_diag[['sk_diagnostico', 'codigo_diagnostico']], left_on='cod_cid_ciap', right_on='codigo_diagnostico', how='left').drop(columns=['codigo_diagnostico'])
    
    # Adicionar surrogate key
    fato['sk_diagnostico_atendimento'] = range(1, len(fato) + 1)
    
    # Selecionar colunas finais
    fato_final = fato[[
        'sk_diagnostico_atendimento',
        'sk_atendimento',
        'sk_paciente',
        'sk_diagnostico',
        'sk_tempo',
        'sk_unidade_saude',
        'diagnosticar_por',
        'e_diag_infeccioso'
    ]].copy()
    
    # Salvar
    output_file = gold_path / 'fato_diagnostico.parquet'
    fato_final.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
    
    print(f"  ✓ fato_diagnostico criada: {len(fato_final):,} registros")
    print(f"    - Diagnósticos infecciosos: {fato_final['e_diag_infeccioso'].sum():,}")
    
    return fato_final


def criar_fato_atendimento_resumo(silver_path, gold_path, dimensoes):
    """
    Cria tabela fato agregada de atendimentos.
    """
    print("\n[FATO 3/3] Criando fato_atendimento_resumo...")
    
    # Desempacotar dimensões
    dim_tempo, dim_unidade, dim_atend, dim_pac, dim_med, dim_diag = dimensoes
    
    # Ler tabelas
    atend = pd.read_parquet(silver_path / 'TAB_ATENDIMENTO.parquet')
    atend_analise = pd.read_parquet(silver_path / 'TAB_ATENDIMENTO_ANALISE.parquet')
    med_prescrito = pd.read_parquet(silver_path / 'TAB_MED_PRESCRITO.parquet')
    med_analise = pd.read_parquet(silver_path / 'TAB_MEDPRESCRITO_ANALISE.parquet')
    
    # Base: atendimentos únicos de TAB_ATENDIMENTO_ANALISE
    fato = atend_analise[['cod_atendimento', 'cod_paciente', 'data_atendimento', 'especialidade']].drop_duplicates()
    fato['data_atendimento'] = pd.to_datetime(fato['data_atendimento'], errors='coerce')
    
    # Agregar diagnósticos por atendimento
    diag_agg = atend_analise.groupby('cod_atendimento').agg({
        'cod_cid_ciap': 'count',  # total de diagnósticos
        'e_diag_infeccioso': 'sum'  # total de diagnósticos infecciosos
    }).rename(columns={
        'cod_cid_ciap': 'total_diagnosticos',
        'e_diag_infeccioso': 'total_diagnosticos_infecciosos'
    })
    
    # Pegar primeiro diagnóstico como principal
    primeiro_diag = atend_analise.groupby('cod_atendimento').first()['cod_cid_ciap']
    
    # Agregar medicamentos por atendimento
    med_agg = med_prescrito.groupby('cod_atendimento').size().rename('total_medicamentos_prescritos')
    
    # Agregar antibióticos
    atb_agg = med_analise.groupby('cod_atendimento').agg({
        'e_antibiotico': 'sum'
    }).rename(columns={'e_antibiotico': 'total_antibioticos_prescritos'})
    
    # Merge agregações
    fato = fato.merge(diag_agg, on='cod_atendimento', how='left')
    fato = fato.merge(primeiro_diag, on='cod_atendimento', how='left', suffixes=('', '_principal'))
    fato = fato.merge(med_agg, on='cod_atendimento', how='left')
    fato = fato.merge(atb_agg, on='cod_atendimento', how='left')
    
    # Obter cod_unidade_saude de TAB_ATENDIMENTO
    unidade_info = atend[['cod_atendimento', 'cod_unidade_saude']].drop_duplicates()
    fato = fato.merge(unidade_info, on='cod_atendimento', how='left')
    
    # Preencher NaN com 0
    fato = fato.fillna({
        'total_diagnosticos': 0,
        'total_diagnosticos_infecciosos': 0,
        'total_medicamentos_prescritos': 0,
        'total_antibioticos_prescritos': 0
    })
    
    # Substituir business keys por surrogate keys
    fato = fato.merge(dim_tempo[['sk_tempo', 'data_completa']], left_on='data_atendimento', right_on='data_completa', how='left').drop(columns=['data_completa'])
    fato = fato.merge(dim_unidade[['sk_unidade_saude', 'cod_unidade_saude']], on='cod_unidade_saude', how='left')
    fato = fato.merge(dim_atend[['sk_atendimento', 'cod_atendimento']], on='cod_atendimento', how='left')
    fato = fato.merge(dim_pac[['sk_paciente', 'cod_paciente']], on='cod_paciente', how='left')
    fato = fato.merge(dim_diag[['sk_diagnostico', 'codigo_diagnostico']], left_on='cod_cid_ciap', right_on='codigo_diagnostico', how='left').drop(columns=['codigo_diagnostico'])
    
    # Criar flags
    fato['teve_prescricao_antibiotico'] = fato['total_antibioticos_prescritos'] > 0
    fato['teve_diagnostico_infeccioso'] = fato['total_diagnosticos_infecciosos'] > 0
    
    # Selecionar colunas finais
    fato_final = fato[[
        'sk_atendimento',
        'sk_paciente',
        'sk_tempo',
        'sk_unidade_saude',
        'sk_diagnostico',
        'especialidade',
        'total_diagnosticos',
        'total_medicamentos_prescritos',
        'total_antibioticos_prescritos',
        'total_diagnosticos_infecciosos',
        'teve_prescricao_antibiotico',
        'teve_diagnostico_infeccioso'
    ]].copy()
    
    # Salvar
    output_file = gold_path / 'fato_atendimento_resumo.parquet'
    fato_final.to_parquet(output_file, engine='pyarrow', compression='snappy', index=False)
    
    print(f"  ✓ fato_atendimento_resumo criada: {len(fato_final):,} registros")
    print(f"    - Com prescrição de antibiótico: {fato_final['teve_prescricao_antibiotico'].sum():,}")
    print(f"    - Com diagnóstico infeccioso: {fato_final['teve_diagnostico_infeccioso'].sum():,}")
    
    return fato_final


# ============================================================================
# VALIDAÇÕES
# ============================================================================

def validar_integridade_referencial(gold_path):
    """
    Valida integridade referencial entre fatos e dimensões.
    """
    print("\n" + "="*80)
    print("VALIDAÇÃO DE INTEGRIDADE REFERENCIAL")
    print("="*80)
    
    # Carregar dimensões
    dim_tempo = pd.read_parquet(gold_path / 'dim_tempo.parquet')
    dim_unidade = pd.read_parquet(gold_path / 'dim_unidade_saude.parquet')
    dim_atend = pd.read_parquet(gold_path / 'dim_atendimento.parquet')
    dim_pac = pd.read_parquet(gold_path / 'dim_paciente.parquet')
    dim_med = pd.read_parquet(gold_path / 'dim_medicamento.parquet')
    dim_diag = pd.read_parquet(gold_path / 'dim_diagnostico.parquet')
    
    # Carregar fatos
    fato_presc = pd.read_parquet(gold_path / 'fato_prescricao.parquet')
    fato_diag = pd.read_parquet(gold_path / 'fato_diagnostico.parquet')
    fato_atend = pd.read_parquet(gold_path / 'fato_atendimento_resumo.parquet')
    
    erros = []
    
    # Validar fato_prescricao
    print("\n[1] Validando fato_prescricao...")
    if not fato_presc['sk_tempo'].dropna().isin(dim_tempo['sk_tempo']).all():
        erros.append("fato_prescricao: FKs inválidas em sk_tempo")
    if not fato_presc['sk_paciente'].dropna().isin(dim_pac['sk_paciente']).all():
        erros.append("fato_prescricao: FKs inválidas em sk_paciente")
    if not fato_presc['sk_medicamento'].dropna().isin(dim_med['sk_medicamento']).all():
        erros.append("fato_prescricao: FKs inválidas em sk_medicamento")
    
    if not erros:
        print("  ✓ Integridade referencial OK")
    
    # Validar fato_diagnostico
    print("\n[2] Validando fato_diagnostico...")
    if not fato_diag['sk_tempo'].dropna().isin(dim_tempo['sk_tempo']).all():
        erros.append("fato_diagnostico: FKs inválidas em sk_tempo")
    if not fato_diag['sk_paciente'].dropna().isin(dim_pac['sk_paciente']).all():
        erros.append("fato_diagnostico: FKs inválidas em sk_paciente")
    
    if len(erros) == 0:
        print("  ✓ Integridade referencial OK")
    
    # Validar fato_atendimento_resumo
    print("\n[3] Validando fato_atendimento_resumo...")
    if not fato_atend['sk_atendimento'].dropna().isin(dim_atend['sk_atendimento']).all():
        erros.append("fato_atendimento_resumo: FKs inválidas em sk_atendimento")
    
    if len(erros) == 0:
        print("  ✓ Integridade referencial OK")
    
    if erros:
        print("\n⚠️  ERROS ENCONTRADOS:")
        for erro in erros:
            print(f"  - {erro}")
        return False
    else:
        print("\n✓ TODAS AS VALIDAÇÕES PASSARAM!")
        return True


# ============================================================================
# FUNÇÃO PRINCIPAL
# ============================================================================

def main():
    """Função principal"""
    print("\n" + "#"*80)
    print("# CAMO-NET ETL PIPELINE - SILVER → GOLD")
    print("# Modelo Dimensional para Análises AMR")
    print("#"*80)
    
    # Definir caminhos
    project_root = Path(__file__).parent.parent.parent
    silver_path = project_root / 'data' / 'silver'
    gold_path = project_root / 'data' / 'gold'
    
    # Validar se pasta silver existe
    if not silver_path.exists():
        print(f"\n❌ ERRO: Pasta silver não encontrada: {silver_path}")
        sys.exit(1)
    
    # Criar pasta gold
    gold_path.mkdir(parents=True, exist_ok=True)
    
    print(f"\nPasta Silver: {silver_path}")
    print(f"Pasta Gold: {gold_path}")
    
    # Criar dimensões
    print("\n" + "="*80)
    print("CRIANDO DIMENSÕES")
    print("="*80)
    
    dim_tempo = criar_dim_tempo(silver_path, gold_path)
    dim_unidade = criar_dim_unidade_saude(silver_path, gold_path)
    dim_atend = criar_dim_atendimento(silver_path, gold_path)
    dim_pac = criar_dim_paciente(silver_path, gold_path)
    dim_med = criar_dim_medicamento(silver_path, gold_path)
    dim_diag = criar_dim_diagnostico(silver_path, gold_path)
    
    dimensoes = (dim_tempo, dim_unidade, dim_atend, dim_pac, dim_med, dim_diag)
    
    # Criar fatos
    print("\n" + "="*80)
    print("CRIANDO TABELAS FATO")
    print("="*80)
    
    fato_presc = criar_fato_prescricao(silver_path, gold_path, dimensoes)
    fato_diag = criar_fato_diagnostico(silver_path, gold_path, dimensoes)
    fato_atend = criar_fato_atendimento_resumo(silver_path, gold_path, dimensoes)
    
    # Validar
    validacao_ok = validar_integridade_referencial(gold_path)
    
    # Resumo final
    print("\n" + "="*80)
    print("RESUMO FINAL")
    print("="*80)
    
    print("\nDimensões criadas:")
    print(f"  - dim_tempo: {len(dim_tempo):,} registros")
    print(f"  - dim_unidade_saude: {len(dim_unidade):,} registros")
    print(f"  - dim_atendimento: {len(dim_atend):,} registros")
    print(f"  - dim_paciente: {len(dim_pac):,} registros")
    print(f"  - dim_medicamento: {len(dim_med):,} registros")
    print(f"  - dim_diagnostico: {len(dim_diag):,} registros")
    
    print("\nTabelas Fato criadas:")
    print(f"  - fato_prescricao: {len(fato_presc):,} registros")
    print(f"  - fato_diagnostico: {len(fato_diag):,} registros")
    print(f"  - fato_atendimento_resumo: {len(fato_atend):,} registros")
    
    print(f"\n{'='*80}\n")
    
    sys.exit(0 if validacao_ok else 1)


if __name__ == '__main__':
    main()
