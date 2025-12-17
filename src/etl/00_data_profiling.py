#!/usr/bin/env python3
"""
CAMO-Net Data Profiling Script
===============================
Script de profiling de qualidade de dados para arquivos CSV brutos.

Objetivo: Inspecionar qualidade técnica antes da limpeza.

Autor: Engenheiro de Dados Sênior
Data: 2025-11-26
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Tuple
import sys


def read_csv_safe(filepath: Path) -> Tuple[pd.DataFrame, str]:
    """
    Lê arquivo CSV com fallback de encoding.
    
    Args:
        filepath: Path do arquivo CSV
        
    Returns:
        Tuple contendo (DataFrame, encoding usado)
        
    Raises:
        ValueError: Se não conseguir ler o arquivo
    """
    # Tentar primeiro com latin1 (comum no Brasil)
    encodings = ['latin1', 'utf-8']
    separators = [';', ',']
    
    for encoding in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(filepath, encoding=encoding, sep=sep, low_memory=False)
                # Validar se leu corretamente (mais de 1 coluna)
                if len(df.columns) > 1:
                    return df, encoding
            except Exception:
                continue
    
    raise ValueError(f"Não foi possível ler o arquivo {filepath}")


def identify_data_type_issues(df: pd.DataFrame) -> List[Dict[str, str]]:
    """
    Identifica problemas de tipos de dados.
    
    Args:
        df: DataFrame para análise
        
    Returns:
        Lista de dicionários com problemas identificados
    """
    issues = []
    
    for col in df.columns:
        col_lower = col.lower()
        dtype = df[col].dtype
        
        # Verificar se colunas de data estão como object/string
        if any(keyword in col_lower for keyword in ['data', 'date', 'dt_']):
            if dtype == 'object':
                issues.append({
                    'coluna': col,
                    'problema': 'Coluna de DATA lida como STRING',
                    'tipo_atual': str(dtype),
                    'tipo_esperado': 'datetime64'
                })
        
        # Verificar se IDs estão como float (deveriam ser int ou string)
        if any(keyword in col_lower for keyword in ['id_', 'cod_', 'codigo']):
            if dtype == 'float64':
                # Verificar se tem valores nulos que justificam o float
                null_count = df[col].isnull().sum()
                if null_count == 0:
                    issues.append({
                        'coluna': col,
                        'problema': 'ID lido como FLOAT (deveria ser INT)',
                        'tipo_atual': str(dtype),
                        'tipo_esperado': 'int64 ou string'
                    })
        
        # Verificar se colunas numéricas estão como object
        if any(keyword in col_lower for keyword in ['valor', 'qtd', 'quantidade', 'numero']):
            if dtype == 'object':
                # Tentar converter para verificar se é numérico
                try:
                    pd.to_numeric(df[col], errors='coerce')
                    issues.append({
                        'coluna': col,
                        'problema': 'Coluna NUMÉRICA lida como STRING',
                        'tipo_atual': str(dtype),
                        'tipo_esperado': 'float64 ou int64'
                    })
                except:
                    pass
    
    return issues


def profile_dataframe(df: pd.DataFrame, filename: str) -> Dict:
    """
    Gera perfil completo de um DataFrame.
    
    Args:
        df: DataFrame para profiling
        filename: Nome do arquivo
        
    Returns:
        Dicionário com métricas de profiling
    """
    # Métricas básicas
    total_rows = len(df)
    total_cols = len(df.columns)
    
    # Análise de nulos
    null_counts = df.isnull().sum()
    null_percentages = (null_counts / total_rows * 100).round(2)
    
    # Criar DataFrame de nulos
    null_analysis = pd.DataFrame({
        'Coluna': null_counts.index,
        'Nulos': null_counts.values,
        'Percentual (%)': null_percentages.values
    }).sort_values('Percentual (%)', ascending=False)
    
    # Top 3 colunas com mais nulos
    top_3_nulls = null_analysis.head(3)
    
    # Duplicatas
    duplicate_count = df.duplicated().sum()
    duplicate_percentage = (duplicate_count / total_rows * 100).round(2)
    
    # Problemas de tipo de dado
    type_issues = identify_data_type_issues(df)
    
    return {
        'filename': filename,
        'total_rows': total_rows,
        'total_cols': total_cols,
        'null_analysis': null_analysis,
        'top_3_nulls': top_3_nulls,
        'duplicate_count': duplicate_count,
        'duplicate_percentage': duplicate_percentage,
        'type_issues': type_issues
    }


def print_profile_report(profile: Dict, encoding: str):
    """
    Imprime relatório de profiling formatado.
    
    Args:
        profile: Dicionário com métricas de profiling
        encoding: Encoding usado na leitura
    """
    print(f"\n{'='*80}")
    print(f"PROFILING: {profile['filename']}")
    print(f"{'='*80}")
    
    # Informações básicas
    print(f"\n[1] INFORMAÇÕES GERAIS")
    print(f"    • Encoding: {encoding}")
    print(f"    • Total de Linhas: {profile['total_rows']:,}")
    print(f"    • Total de Colunas: {profile['total_cols']}")
    
    # Duplicatas
    print(f"\n[2] ANÁLISE DE DUPLICATAS")
    print(f"    • Linhas Duplicadas: {profile['duplicate_count']:,}")
    print(f"    • Percentual: {profile['duplicate_percentage']}%")
    
    if profile['duplicate_count'] > 0:
        print(f"    ⚠️  ATENÇÃO: Arquivo contém duplicatas!")
    else:
        print(f"    ✓ Nenhuma duplicata encontrada")
    
    # Top 3 colunas com mais nulos
    print(f"\n[3] TOP 3 COLUNAS COM MAIS VALORES NULOS")
    
    if profile['top_3_nulls']['Nulos'].sum() == 0:
        print(f"    ✓ Nenhum valor nulo encontrado no dataset!")
    else:
        for idx, row in profile['top_3_nulls'].iterrows():
            if row['Nulos'] > 0:
                print(f"    • {row['Coluna']}: {int(row['Nulos']):,} nulos ({row['Percentual (%)']}%)")
    
    # Problemas de tipo de dado
    print(f"\n[4] PROBLEMAS DE TIPO DE DADO")
    
    if not profile['type_issues']:
        print(f"    ✓ Nenhum problema crítico de tipo de dado identificado")
    else:
        print(f"    ⚠️  {len(profile['type_issues'])} problema(s) identificado(s):")
        for issue in profile['type_issues']:
            print(f"\n    • Coluna: {issue['coluna']}")
            print(f"      Problema: {issue['problema']}")
            print(f"      Tipo Atual: {issue['tipo_atual']}")
            print(f"      Tipo Esperado: {issue['tipo_esperado']}")
    
    print(f"\n{'='*80}\n")


def profile_all_csv_files(input_dir: Path):
    """
    Realiza profiling de todos os arquivos CSV em um diretório.
    
    Args:
        input_dir: Path do diretório com arquivos CSV
    """
    print(f"\n{'#'*80}")
    print(f"# CAMO-NET DATA PROFILING REPORT")
    print(f"# Diretório: {input_dir}")
    print(f"{'#'*80}\n")
    
    # Listar todos os CSVs
    csv_files = list(input_dir.glob('*.csv'))
    
    if not csv_files:
        print(f"⚠️  Nenhum arquivo CSV encontrado em {input_dir}")
        return
    
    print(f"Total de arquivos encontrados: {len(csv_files)}\n")
    
    # Armazenar resumo geral
    summary_data = []
    
    # Processar cada arquivo
    for csv_file in sorted(csv_files):
        try:
            # Ler arquivo
            df, encoding = read_csv_safe(csv_file)
            
            # Gerar perfil
            profile = profile_dataframe(df, csv_file.name)
            
            # Imprimir relatório
            print_profile_report(profile, encoding)
            
            # Adicionar ao resumo
            summary_data.append({
                'Arquivo': csv_file.name,
                'Linhas': profile['total_rows'],
                'Colunas': profile['total_cols'],
                'Duplicatas': profile['duplicate_count'],
                'Duplicatas (%)': profile['duplicate_percentage'],
                'Colunas com Nulos': (profile['null_analysis']['Nulos'] > 0).sum(),
                'Problemas de Tipo': len(profile['type_issues']),
                'Encoding': encoding
            })
            
        except Exception as e:
            print(f"\n❌ ERRO ao processar {csv_file.name}: {str(e)}\n")
            summary_data.append({
                'Arquivo': csv_file.name,
                'Linhas': 'ERRO',
                'Colunas': 'ERRO',
                'Duplicatas': 'ERRO',
                'Duplicatas (%)': 'ERRO',
                'Colunas com Nulos': 'ERRO',
                'Problemas de Tipo': 'ERRO',
                'Encoding': 'ERRO'
            })
    
    # Imprimir resumo geral
    print(f"\n{'#'*80}")
    print(f"# RESUMO GERAL DO PROFILING")
    print(f"{'#'*80}\n")
    
    summary_df = pd.DataFrame(summary_data)
    print(summary_df.to_string(index=False))
    
    # Estatísticas gerais
    print(f"\n{'='*80}")
    print(f"ESTATÍSTICAS GERAIS")
    print(f"{'='*80}")
    
    total_files = len(csv_files)
    successful_files = len([s for s in summary_data if s['Linhas'] != 'ERRO'])
    
    print(f"\nArquivos processados: {successful_files}/{total_files}")
    
    if successful_files > 0:
        total_rows = sum(s['Linhas'] for s in summary_data if s['Linhas'] != 'ERRO')
        files_with_duplicates = sum(1 for s in summary_data if s['Duplicatas'] != 'ERRO' and s['Duplicatas'] > 0)
        files_with_type_issues = sum(1 for s in summary_data if s['Problemas de Tipo'] != 'ERRO' and s['Problemas de Tipo'] > 0)
        
        print(f"Total de registros: {total_rows:,}")
        print(f"Arquivos com duplicatas: {files_with_duplicates}")
        print(f"Arquivos com problemas de tipo: {files_with_type_issues}")
    
    print(f"\n{'='*80}\n")


def main():
    """Função principal"""
    # Definir caminho do diretório de entrada
    project_root = Path(__file__).parent.parent.parent
    input_dir = project_root / 'data' / '20250101_carga_inicial_ana_roccio'
    
    # Validar se diretório existe
    if not input_dir.exists():
        print(f"❌ ERRO: Diretório não encontrado: {input_dir}")
        print(f"\nVerificando diretórios alternativos...")
        
        # Tentar caminho alternativo mencionado pelo usuário
        alt_dir = project_root / 'data' / 'raw' / '2025-11-26_carga_inicial'
        if alt_dir.exists():
            print(f"✓ Encontrado diretório alternativo: {alt_dir}")
            input_dir = alt_dir
        else:
            print(f"❌ Diretório alternativo também não encontrado: {alt_dir}")
            sys.exit(1)
    
    # Executar profiling
    profile_all_csv_files(input_dir)


if __name__ == "__main__":
    main()
