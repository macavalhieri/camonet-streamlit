#!/usr/bin/env python3
"""
CAMO-Net ETL Pipeline - Raw to Bronze Layer
============================================
Converte arquivos CSV brutos para formato Parquet otimizado.
Adiciona metadados de ingestão.

Autor: Engenheiro de Dados Sênior
Data: 2025-11-26
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import sys


def read_csv_with_fallback(filepath):
    """
    Tenta ler CSV com diferentes encodings e separadores.
    
    Args:
        filepath: Path do arquivo CSV
        
    Returns:
        tuple: (DataFrame, encoding usado)
    """
    encodings = ['utf-8', 'latin1', 'cp1252']
    separators = [';', ',']
    
    for encoding in encodings:
        for sep in separators:
            try:
                df = pd.read_csv(filepath, encoding=encoding, sep=sep, low_memory=False)
                # Validar se leu corretamente (mais de 1 coluna)
                if len(df.columns) > 1:
                    print(f"  ✓ Lido com encoding={encoding}, sep='{sep}'")
                    return df, encoding
            except Exception:
                continue
    
    raise ValueError(f"Não foi possível ler o arquivo {filepath}")


def process_raw_to_bronze(raw_path, bronze_path):
    """
    Processa todos os CSVs da camada raw para bronze.
    
    Args:
        raw_path: Path da pasta raw
        bronze_path: Path da pasta bronze
    """
    print("="*80)
    print("CAMO-Net ETL Pipeline - RAW → BRONZE")
    print("="*80)
    
    # Criar pasta bronze se não existir
    bronze_path.mkdir(parents=True, exist_ok=True)
    
    # Timestamp de ingestão
    ingestion_timestamp = datetime.now()
    
    # Listar todos os CSVs
    csv_files = list(raw_path.glob('*.csv'))
    
    if not csv_files:
        print(f"\n⚠️  Nenhum arquivo CSV encontrado em {raw_path}")
        return
    
    print(f"\nArquivos encontrados: {len(csv_files)}\n")
    
    # Processar cada arquivo
    results = []
    
    for csv_file in csv_files:
        table_name = csv_file.stem
        print(f"\n{'─'*80}")
        print(f"Processando: {table_name}")
        print(f"{'─'*80}")
        
        try:
            # Ler CSV
            df, encoding = read_csv_with_fallback(csv_file)
            original_rows = len(df)
            original_cols = len(df.columns)
            
            # Adicionar coluna de metadados
            df['_ingestion_date'] = ingestion_timestamp
            
            # Caminho de saída
            output_file = bronze_path / f"{table_name}.parquet"
            
            # Salvar como Parquet
            df.to_parquet(
                output_file,
                engine='pyarrow',
                compression='snappy',
                index=False
            )
            
            # Calcular tamanho dos arquivos
            csv_size = csv_file.stat().st_size / (1024 * 1024)  # MB
            parquet_size = output_file.stat().st_size / (1024 * 1024)  # MB
            compression_ratio = (1 - parquet_size / csv_size) * 100
            
            print(f"  ✓ Salvo em: {output_file.name}")
            print(f"  • Linhas: {original_rows:,}")
            print(f"  • Colunas: {original_cols} + 1 (metadados)")
            print(f"  • Tamanho CSV: {csv_size:.2f} MB")
            print(f"  • Tamanho Parquet: {parquet_size:.2f} MB")
            print(f"  • Compressão: {compression_ratio:.1f}%")
            
            results.append({
                'tabela': table_name,
                'status': 'SUCCESS',
                'linhas': original_rows,
                'colunas': original_cols,
                'encoding': encoding,
                'tamanho_mb': parquet_size
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
        print(f"Tamanho total em bronze: {total_size:.2f} MB")
    
    print(f"\n{'='*80}\n")
    
    return results


def main():
    """Função principal"""
    # Definir caminhos
    project_root = Path(__file__).parent.parent.parent
    raw_path = project_root / 'data' / '20250101_carga_inicial_ana_roccio'
    bronze_path = project_root / 'data' / 'bronze'
    
    # Validar se pasta raw existe
    if not raw_path.exists():
        print(f"❌ ERRO: Pasta raw não encontrada: {raw_path}")
        sys.exit(1)
    
    # Executar processamento
    results = process_raw_to_bronze(raw_path, bronze_path)
    
    # Retornar código de saída
    failed = sum(1 for r in results if r['status'] == 'FAILED')
    sys.exit(0 if failed == 0 else 1)


if __name__ == '__main__':
    main()
