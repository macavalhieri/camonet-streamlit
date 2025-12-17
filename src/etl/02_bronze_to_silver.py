#!/usr/bin/env python3
"""
CAMO-Net ETL Pipeline - Bronze to Silver Layer
===============================================
Aplica limpeza, padronização e anonimização nos dados.

Transformações:
- Padronização de nomes de colunas (snake_case)
- Limpeza de strings (medicamentos)
- Anonimização de dados sensíveis (PII)

Autor: Engenheiro de Dados Sênior
Data: 2025-11-26
"""

import pandas as pd
from pathlib import Path
import hashlib
import re
import sys


def to_snake_case(name):
    """
    Converte string para snake_case.
    
    Args:
        name: String para converter
        
    Returns:
        String em snake_case
    """
    # Remover caracteres especiais
    name = re.sub(r'[^\w\s]', '', name)
    # Substituir espaços por underscore
    name = re.sub(r'\s+', '_', name)
    # Converter para minúsculas
    name = name.lower()
    # Remover underscores duplicados
    name = re.sub(r'_+', '_', name)
    # Remover underscores no início e fim
    name = name.strip('_')
    
    return name


def hash_pii(value):
    """
    Aplica hash SHA256 em dados sensíveis.
    
    Args:
        value: Valor para hash
        
    Returns:
        Hash SHA256 do valor
    """
    if pd.isna(value):
        return None
    
    # Converter para string e aplicar hash
    value_str = str(value)
    return hashlib.sha256(value_str.encode()).hexdigest()


def clean_medication_name(name):
    """
    Limpa e padroniza nomes de medicamentos.
    
    Args:
        name: Nome do medicamento
        
    Returns:
        Nome padronizado
    """
    if pd.isna(name):
        return None
    
    # Converter para string, remover espaços extras e converter para maiúsculas
    cleaned = str(name).strip().upper()
    
    # Remover espaços duplicados
    cleaned = re.sub(r'\s+', ' ', cleaned)
    
    return cleaned


def identify_pii_columns(df):
    """
    Identifica colunas que podem conter dados sensíveis.
    
    Args:
        df: DataFrame
        
    Returns:
        Lista de colunas PII
    """
    pii_keywords = [
        'nome', 'cpf', 'rg', 'cns', 'telefone', 'email', 
        'endereco', 'endereço', 'logradouro', 'paciente_nome'
    ]
    
    pii_columns = []
    
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in pii_keywords):
            # Verificar se não é uma coluna de ID já anonimizada
            if 'id' not in col_lower or 'nome' in col_lower:
                pii_columns.append(col)
    
    return pii_columns


def identify_medication_columns(df):
    """
    Identifica colunas que contêm nomes de medicamentos.
    
    Args:
        df: DataFrame
        
    Returns:
        Lista de colunas de medicamentos
    """
    med_keywords = [
        'medicamento', 'med', 'principio', 'substancia', 
        'nome_med', 'descricao_med'
    ]
    
    med_columns = []
    
    for col in df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in med_keywords):
            # Verificar se não é ID
            if 'id' not in col_lower or 'nome' in col_lower or 'descricao' in col_lower:
                med_columns.append(col)
    
    return med_columns


def process_bronze_to_silver(bronze_path, silver_path):
    """
    Processa todos os Parquets da camada bronze para silver.
    
    Args:
        bronze_path: Path da pasta bronze
        silver_path: Path da pasta silver
    """
    print("="*80)
    print("CAMO-Net ETL Pipeline - BRONZE → SILVER")
    print("="*80)
    
    # Criar pasta silver se não existir
    silver_path.mkdir(parents=True, exist_ok=True)
    
    # Listar todos os Parquets
    parquet_files = list(bronze_path.glob('*.parquet'))
    
    if not parquet_files:
        print(f"\n⚠️  Nenhum arquivo Parquet encontrado em {bronze_path}")
        return
    
    print(f"\nArquivos encontrados: {len(parquet_files)}\n")
    
    # Processar cada arquivo
    results = []
    
    for parquet_file in parquet_files:
        table_name = parquet_file.stem
        print(f"\n{'─'*80}")
        print(f"Processando: {table_name}")
        print(f"{'─'*80}")
        
        try:
            # Ler Parquet
            df = pd.read_parquet(parquet_file)
            original_rows = len(df)
            original_cols = len(df.columns)
            
            print(f"  • Linhas: {original_rows:,}")
            print(f"  • Colunas originais: {original_cols}")
            
            # 1. Padronizar nomes das colunas
            print(f"\n  [1] Padronizando nomes das colunas...")
            old_columns = df.columns.tolist()
            new_columns = [to_snake_case(col) for col in old_columns]
            df.columns = new_columns
            
            renamed_count = sum(1 for old, new in zip(old_columns, new_columns) if old != new)
            print(f"      ✓ {renamed_count} colunas renomeadas para snake_case")
            
            # 2. Limpar nomes de medicamentos
            print(f"\n  [2] Limpando nomes de medicamentos...")
            med_columns = identify_medication_columns(df)
            
            if med_columns:
                print(f"      • Colunas identificadas: {med_columns}")
                for col in med_columns:
                    df[col] = df[col].apply(clean_medication_name)
                print(f"      ✓ {len(med_columns)} colunas de medicamentos padronizadas")
            else:
                print(f"      • Nenhuma coluna de medicamento identificada")
            
            # 3. Anonimizar dados sensíveis
            print(f"\n  [3] Anonimizando dados sensíveis (PII)...")
            pii_columns = identify_pii_columns(df)
            
            if pii_columns:
                print(f"      • Colunas PII identificadas: {pii_columns}")
                
                for col in pii_columns:
                    # Opção 1: Aplicar hash (mantém referência)
                    # Opção 2: Remover coluna (mais seguro)
                    
                    # Verificar se é nome de paciente ou CPF (remover)
                    if any(keyword in col.lower() for keyword in ['nome', 'cpf']):
                        print(f"      ⚠️  Removendo coluna: {col}")
                        df = df.drop(columns=[col])
                    else:
                        # Aplicar hash para outros dados sensíveis
                        print(f"      🔒 Aplicando hash em: {col}")
                        df[col] = df[col].apply(hash_pii)
                
                print(f"      ✓ {len(pii_columns)} colunas PII tratadas")
            else:
                print(f"      • Nenhuma coluna PII identificada")
            
            # 4. Salvar em Silver
            output_file = silver_path / f"{table_name}.parquet"
            df.to_parquet(
                output_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            final_cols = len(df.columns)
            file_size = output_file.stat().st_size / (1024 * 1024)  # MB
            
            print(f"\n  ✓ Salvo em: {output_file.name}")
            print(f"  • Colunas finais: {final_cols}")
            print(f"  • Tamanho: {file_size:.2f} MB")
            
            results.append({
                'tabela': table_name,
                'status': 'SUCCESS',
                'linhas': original_rows,
                'colunas_antes': original_cols,
                'colunas_depois': final_cols,
                'tamanho_mb': file_size
            })
            
        except Exception as e:
            print(f"  ✗ ERRO: {str(e)}")
            results.append({
                'tabela': table_name,
                'status': 'FAILED',
                'erro': str(e)
            })
    
    # Resumo final
    print(f"\n{'='*80}")
    print("RESUMO DA EXECUÇÃO")
    print(f"{'='*80}\n")
    
    success_count = sum(1 for r in results if r['status'] == 'SUCCESS')
    failed_count = sum(1 for r in results if r['status'] == 'FAILED')
    
    print(f"Total de arquivos processados: {len(results)}")
    print(f"  ✓ Sucesso: {success_count}")
    print(f"  ✗ Falhas: {failed_count}")
    
    if success_count > 0:
        total_rows = sum(r.get('linhas', 0) for r in results if r['status'] == 'SUCCESS')
        total_size = sum(r.get('tamanho_mb', 0) for r in results if r['status'] == 'SUCCESS')
        print(f"\nTotal de registros processados: {total_rows:,}")
        print(f"Tamanho total em silver: {total_size:.2f} MB")
    
    print(f"\n{'='*80}\n")
    
    return results


def main():
    """Função principal"""
    # Definir caminhos
    project_root = Path(__file__).parent.parent.parent
    bronze_path = project_root / 'data' / 'bronze'
    silver_path = project_root / 'data' / 'silver'
    
    # Validar se pasta bronze existe
    if not bronze_path.exists():
        print(f"❌ ERRO: Pasta bronze não encontrada: {bronze_path}")
        print(f"Execute primeiro o script raw_to_bronze.py")
        sys.exit(1)
    
    # Executar processamento
    results = process_bronze_to_silver(bronze_path, silver_path)
    
    # Retornar código de saída
    failed = sum(1 for r in results if r['status'] == 'FAILED')
    sys.exit(0 if failed == 0 else 1)


if __name__ == '__main__':
    main()
