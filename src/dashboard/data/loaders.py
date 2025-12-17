# src/camonet_dashboard/data/loaders.py

from __future__ import annotations

from pathlib import Path
import pandas as pd
import requests
import streamlit as st

_CACHE_DIR = Path('/tmp/camonet_cache')
_CACHE_FILE = _CACHE_DIR / 'full_data.parquet'


def load_gold_data() -> pd.DataFrame:
    """
    Carrega o dataset Gold (Parquet) a partir de uma URL (Google Drive direct download),
    com cache local para evitar downloads repetidos no Streamlit Cloud.
    """
    data_url = st.secrets['DATA_URL']

    _CACHE_DIR.mkdir(parents=True, exist_ok=True)

    if not _CACHE_FILE.exists():
        r = requests.get(data_url, timeout=120)
        r.raise_for_status()

        # Se o Drive não estiver público, pode vir HTML em vez de parquet
        ctype = (r.headers.get('Content-Type') or '').lower()
        if 'text/html' in ctype:
            raise RuntimeError(
                'Download do Google Drive retornou HTML (provável falta de permissão pública). '
                'Verifique se o arquivo está como "Anyone with the link" (Viewer).'
            )

        _CACHE_FILE.write_bytes(r.content)

    return pd.read_parquet(_CACHE_FILE)

