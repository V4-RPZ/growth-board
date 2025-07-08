import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
import plotly.express as px
import os
from google.cloud import bigquery
from google.oauth2 import service_account
import calendar
import re
import requests
import json

# --- CONFIGURAÇÕES GERAIS CORRIGIDAS (Lendo do st.secrets) ---
BQ_PROJECT_ID_GCP = st.secrets["BQ_PROJECT_ID_GCP"]
BQ_DATASET_ID = st.secrets["BQ_DATASET_ID"]
BQ_TABLE_GKW = st.secrets["BQ_TABLE_GKW"]
BQ_TABLE_GADS = st.secrets["BQ_TABLE_GADS"]
BQ_TABLE_FBADS = st.secrets["BQ_TABLE_FBADS"]
BQ_TABLE_LEADS = st.secrets["BQ_TABLE_LEADS"]
URL_PLANILHA_2 = st.secrets["URL_PLANILHA_2"]
WEBHOOK_CHECKIN = st.secrets["WEBHOOK_CHECKIN"]
WEBHOOK_RELATORIO = st.secrets["WEBHOOK_RELATORIO"]
WEBHOOK_HIPOTESES = st.secrets["WEBHOOK_HIPOTESES"]


HR_SEPARATOR_STYLE = "<hr style='border-top: 2px solid #D33682; margin-top: 25px; margin-bottom: 25px;'>"


# --- MAPEAMENTO DE COLUNAS (CORRIGIDO E PADRONIZADO) ---
COLUNAS_BQ_LEADS_MAP = {
    "Data": "data_criacao",
    "Qualificação": "qualificacao_pl",
    "utm_term": "utm_term_pl",
    "utm_campaign": "utm_campaign_pl",
    "utm_source": "utm_source_pl",
    "Valor": "real_faturamento_venda_pl",
    "project_id": "project_id_pl"
}

COLUNAS_PLANILHA_2_MAP = {
    "Cliente": "cliente_p2",
    "project_id": "project_id_p2",
    "Data": "data_meta_p2",
    "Faturamento": "meta_faturamento_p2",
    "Ticket médio": "meta_ticket_medio_p2",
    "Investimento": "meta_investimento_p2",
    "Leads → MQL": "meta_conv_lead_mql_p2",
    "Leads": "meta_leads_p2",
    "MQL": "meta_mql_p2",
    "Vendas": "meta_vendas_p2",
    "CPL": "meta_cpl_p2",
    "CPMQL": "meta_cpmql_p2",
    "LT": "lt_p2",
    "M.C": "mc_p2",
    "Fee": "fee_p2",
    "Growth Rate": "meta_growth_rate_p2",
    "ROI": "meta_roi_p2",
    "ROAS": "meta_roas_p2"
}


COLUNAS_BQ_KEYWORDS_MAP = {
    'date': 'data_api_gkw',
    'cost': 'cost_gkw',
    'project_id': 'project_id_gkw',
    'keyword_text': 'keyword_text_gkw',
    'impressions': 'impressions_gkw',
    'clicks': 'clicks_gkw',
    'campaign_name': 'campaign_name_gkw',
    'ad_group_name': 'ad_group_name_gkw',
    'ad_id': 'ad_id_gkw'
}

COLUNAS_BQ_GADS_MAP = {
    'date': 'data_api_gads',
    'cost': 'cost_gads',
    'project_id': 'project_id_gads',
    'impressions': 'impressions_gads',
    'clicks': 'clicks_gads',
    'campaign_name': 'campaign_name_gads',
    'campaign_id': 'campaign_id_gads'
}


COLUNAS_BQ_FBADS_MAP = {
    'ad_id': 'ad_id_fb',
    'ad_name': 'Criativo_fb_ad_name',
    'adset_name': 'adset_name_fb',
    'campaign_name': 'campaign_name_fb',
    'spend': 'Investido_fb_val',
    'impressions': 'Impressões_fb_val',
    'clicks': 'Cliques_fb_val',
    'date_start': 'data_api_fbads',
    'project_id': 'project_id_fb'
}

CORES_STATUS_TEXTO = {
    "Saudável": "#39aa00",
    "Atenção": "#e79e00",
    "Ruim": "#9a0000"
}

CORES_STATUS_BORDA = {
    "Saudável": "#28a745",
    "Atenção": "#ffc107",
    "Ruim": "#dc3545",
    "N/A": "#6c757d"
}

COMMON_TABLE_HEADER_STYLES = [
    {'selector': 'th', 'props': [
        ('background-color', '#333333 !important'),
        ('color', 'white !important'),
        ('font-weight', 'bold !important'),
        ('text-align', 'left !important')
    ]},
]


# --- FUNÇÕES DE FORMATAÇÃO E PROCESSAMENTO ---

def format_brazilian(value, format_string):
    """Formata um número para o padrão brasileiro (ex: 1.234,56)."""
    if pd.isna(value) or value == float('inf') or value == float('-inf'):
        return "N/A"
    try:
        formatted_value = format_string.format(value)
        return formatted_value.replace(",", "TEMP").replace(".", ",").replace("TEMP", ".")
    except (ValueError, TypeError):
        return str(value)

def clean_and_round_payload(d):
    """Recursivamente limpa e arredonda valores em um dicionário/lista para o payload do webhook."""
    if d is None: return 0
    if isinstance(d, dict): return {k: clean_and_round_payload(v) for k, v in d.items()}
    if isinstance(d, list): return [clean_and_round_payload(i) for i in d]
    if isinstance(d, (float, np.floating)):
        if pd.isna(d) or np.isinf(d): return 0.0
        return round(float(d), 2)
    if isinstance(d, str) and d == 'inf': return 0.0
    if pd.api.types.is_number(d) and not isinstance(d, bool):
        if pd.isna(d) or np.isinf(d): return 0
        return round(float(d), 2)
    return d

def normalize_text_series(series):
    """Converte uma série de strings para minúsculas e remove acentos."""
    if series.empty or not pd.api.types.is_string_dtype(series): return series
    return series.str.lower().str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8')


@st.cache_data(ttl=3600)
def fetch_data_from_bigquery(project_gcp_id, dataset_id, table_id,
                             column_mapping, date_column_in_bq,
                             start_date_dt_func, end_date_dt_func,
                             project_id_api_column_in_bq=None, project_id_api_value_filter=None):
    if not all([project_gcp_id, dataset_id, table_id, column_mapping]):
        st.warning(f"Configurações do BigQuery incompletas para buscar dados da tabela {table_id}.")
        return pd.DataFrame()
    try:
        credentials_info = st.secrets["gcp_service_account"]
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        client = bigquery.Client(credentials=credentials, project=project_gcp_id)
        start_date_str = start_date_dt_func.strftime("%Y-%m-%d")
        end_date_str = end_date_dt_func.strftime("%Y-%m-%d")
        cols_bq_needed_original = [k for k in column_mapping.keys() if k is not None]
        if not cols_bq_needed_original:
            st.warning(f"Mapeamento de colunas para {table_id} está vazio ou inválido.")
            return pd.DataFrame()
        colunas_bq_select = ", ".join([f"`{col}`" for col in cols_bq_needed_original])
        query = f"""
            SELECT {colunas_bq_select}
            FROM `{project_gcp_id}.{dataset_id}.{table_id}`
            WHERE {date_column_in_bq} >= DATE("{start_date_str}")
            AND {date_column_in_bq} <= DATE("{end_date_str}")
        """
        if project_id_api_column_in_bq and project_id_api_value_filter:
            query += f' AND {project_id_api_column_in_bq} = "{project_id_api_value_filter}"'
        query_job = client.query(query, timeout=60)
        df = query_job.to_dataframe()
        if df.empty: return pd.DataFrame()
        valid_rename_map = {k: v for k, v in column_mapping.items() if k in df.columns}
        df_renamed = df.rename(columns=valid_rename_map)
        date_cols = ['data_criacao', 'data_api_gkw', 'data_api_gads', 'data_api_fbads']
        for col in date_cols:
            if col in df_renamed.columns: df_renamed[col] = pd.to_datetime(df_renamed[col], errors='coerce')
        numeric_cols = {'real_faturamento_venda_pl': float, 'cost_gkw': float, 'cost_gads': float, 'Investido_fb_val': float}
        for col, dtype in numeric_cols.items():
            if col in df_renamed.columns: df_renamed[col] = pd.to_numeric(df_renamed[col], errors='coerce').fillna(0)
        integer_cols = ['impressions_gkw', 'clicks_gkw', 'impressions_gads', 'clicks_gads', 'Impressões_fb_val', 'Cliques_fb_val']
        for col in integer_cols:
             if col in df_renamed.columns: df_renamed[col] = pd.to_numeric(df_renamed[col], errors='coerce').fillna(0).astype(int)
        string_cols_to_convert = ['utm_source_pl', 'utm_campaign_pl', 'utm_term_pl', 'campaign_id_gads', 'keyword_text_gkw', 'campaign_name_gkw', 'ad_group_name_gkw', 'ad_id_gkw', 'campaign_name_gads', 'ad_id_fb', 'Criativo_fb_ad_name', 'adset_name_fb', 'campaign_name_fb']
        for col in string_cols_to_convert:
            if col in df_renamed.columns: df_renamed[col] = df_renamed[col].astype(str).fillna('')
        final_cols_present = [v for k, v in valid_rename_map.items()]
        df_final = df_renamed[[col for col in final_cols_present if col in df_renamed.columns]].copy()
        return df_final
    except (bigquery.dbapi.Error, bigquery.exceptions.GoogleAPICallError) as e:
        st.error(f"Erro de API ao consultar o BigQuery para a tabela {table_id}: {e}")
        st.info("O serviço do Google pode estar temporariamente indisponível ou a query é inválida.")
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Erro inesperado ao carregar dados do BigQuery ({table_id}): {e}")
        return pd.DataFrame()

@st.cache_data(ttl=120)
def carregar_planilha_gs(url_planilha, colunas_map, nome_coluna_data_renomeada, nome_planilha_debug=""):
    try:
        url_planilha_no_cache = f"{url_planilha}&v={datetime.now().timestamp()}"
        df = pd.read_csv(url_planilha_no_cache, dayfirst=True, dtype=str)
        colunas_originais_para_manter = [col_original for col_original in colunas_map.keys() if col_original in df.columns]
        if not colunas_originais_para_manter:
            st.warning(f"Planilha ({nome_planilha_debug}): Nenhuma das colunas mapeadas foi encontrada.")
            return pd.DataFrame()
        df_processado = df[colunas_originais_para_manter].copy()
        df_processado.rename(columns=colunas_map, inplace=True)
        if nome_coluna_data_renomeada in df_processado.columns:
            df_processado[nome_coluna_data_renomeada] = pd.to_datetime(df_processado[nome_coluna_data_renomeada], errors='coerce', dayfirst=True)
            df_processado.dropna(subset=[nome_coluna_data_renomeada], inplace=True)
        else:
            st.warning(f"Planilha ({nome_planilha_debug}): Coluna de data principal '{nome_coluna_data_renomeada}' não encontrada.")
            return pd.DataFrame()
        if df_processado.empty: return pd.DataFrame()
        df_processado['ano_mes'] = df_processado[nome_coluna_data_renomeada].dt.to_period('M')
        if nome_planilha_debug == "Planilha 2 (Metas)":
            cols_valor_ou_contagem_p2 = ["meta_faturamento_p2", "meta_investimento_p2", "meta_leads_p2", "meta_mql_p2", "meta_vendas_p2", "meta_ticket_medio_p2", "meta_cpl_p2", "meta_cpmql_p2", "lt_p2", "fee_p2", "meta_growth_rate_p2", "meta_roi_p2", "meta_roas_p2"]
            cols_percentual_meta_p2 = ["meta_conv_lead_mql_p2", "mc_p2"]
            for col_meta in df_processado.columns:
                if col_meta in cols_valor_ou_contagem_p2 and df_processado[col_meta].notna().any():
                    df_processado[col_meta] = df_processado[col_meta].astype(str).str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).str.strip()
                    df_processado[col_meta] = pd.to_numeric(df_processado[col_meta], errors='coerce').fillna(0)
                elif col_meta in cols_percentual_meta_p2 and df_processado[col_meta].notna().any():
                    df_processado[col_meta] = df_processado[col_meta].astype(str).str.replace('%', '', regex=False).str.replace(',', '.', regex=False).str.strip()
                    df_processado[col_meta] = pd.to_numeric(df_processado[col_meta], errors='coerce').fillna(0) / 100
        return df_processado
    except Exception as e:
        st.error(f"Erro crítico ao carregar/processar planilha ({nome_planilha_debug}): {e}")
        return pd.DataFrame()

def render_persistent_sidebar():
    st.sidebar.markdown("<div style='text-align: center;'><img src='https://i.postimg.cc/dVjMB4jK/LOGO-RPZ-BRANCO.png' width='250'></div>", unsafe_allow_html=True)
    st.sidebar.header("Filtros")
    if st.sidebar.button("Limpar Cache de Dados"):
        st.cache_data.clear()
        st.sidebar.success("O cache foi limpo! Os dados serão recarregados.")
        st.rerun()
    client_map_dict = st.session_state.get('client_map_dict', {})
    if not client_map_dict:
        st.sidebar.warning("Mapeamento de clientes não encontrado.")
        st.stop()
    lista_clientes = sorted(list(client_map_dict.keys()))
    selected_client = st.sidebar.selectbox("Selecione o Cliente", options=lista_clientes, key="selected_client")
    start_date = st.sidebar.date_input("Data Inicial", key="start_date")
    end_date = st.sidebar.date_input("Data Final", key="end_date")
    conferidor_mode = st.sidebar.toggle("Conferidor", key="conferidor_mode")
    if start_date > end_date and end_date is not None: # Adicionado check de None
        st.sidebar.error("A 'Data Inicial' não pode ser posterior à 'Data Final'.")
        st.stop()
    project_id = client_map_dict.get(selected_client)
    return selected_client, project_id, start_date, end_date, conferidor_mode

# --- CONFIGURAÇÃO DA PÁGINA E CSS ---
st.set_page_config(layout="wide", page_title="Growth Board")

# --- CSS ATUALIZADO ---
st.markdown("""
<style>
    .main .block-container { background-color: #141414; }
    [data-testid="stMetric"] { background-color: #2A2A2A; border: 1px solid #2A2A2A; border-radius: 10px; padding: 15px; }
    [data-testid="stMetricValue"] { color: #FFFFFF; }
    [data-testid="stMetricDelta"] > div[data-testid="stMarkdownContainer"] > p { color: #FFFFFF !important; font-weight: bold; }

    .performance-kpi-card {
        background-color: #2A2A2A;
        border-radius: 10px;
        padding: 15px 20px;
        border-left: 10px solid;
        height: 100%;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
        margin-bottom: 16px; 
    }
    .kpi-header { display: flex; justify-content: space-between; align-items: flex-start; width: 100%; margin-bottom: 5px; }
    .kpi-metric-name { font-size: 1.1em; font-weight: bold; color: #FFFFFF; }
    .kpi-main-value { font-size: 2.2rem; color: #FFFFFF; font-weight: 600; line-height: 1.1; }
    .kpi-details-grid { margin-top: 10px; font-size: 0.95em; color: #E0E0E0; }
    .kpi-percent-achieved { margin-top: 10px; font-size: 1.1em; color: #FFFFFF; font-weight: bold; }
    .kpi-help-icon { width: 18px; height: 18px; border-radius: 50%; background-color: #555; color: #141414; text-align: center; font-size: 13px; line-height: 18px; font-weight: bold; cursor: help; flex-shrink: 0; }
    
    .metric-card-saudavel { border-left-color: #28a745 !important; }
    .metric-card-atencao { border-left-color: #ffc107 !important; }
    .metric-card-ruim { border-left-color: #dc3545 !important; }
    .metric-card-na { border-left-color: #6c757d !important; }
    
    @media (max-width: 768px) {
        h1 { font-size: 1.8rem; }
        h2 { font-size: 1.5rem; }
    }
    
    h1, h2, h3 { color: #FFFFFF; text-align: center; }
    h1 { padding-bottom: 2px; font-size: 2.5rem; }
    h2 { padding-bottom: 8px; }
    table { width: 100%; color: white; background-color: #141414 !important; border-collapse: collapse; }
    th { background-color: #333333 !important; color: white !important; font-weight: bold !important; text-align: left !important; padding: 8px; border: 1px solid #444444 !important; }
    td { padding: 8px; border: 1px solid #444444 !important; text-align: left !important; }
    .stButton>button { background-color: #D33682; color: white; border-radius: 5px; padding: 10px 20px; border: none; font-weight: bold; width: 100%; display: block; margin: 0 auto; }
    .stButton>button:hover { background-color: #b52a6e; }
    .metric-block { margin-bottom: 20px; padding: 15px; background-color: #2A2A2A; border-radius: 8px; height: 250px; display: flex; flex-direction: column; justify-content: center; }
    .metric-block p { margin-bottom: 8px; font-size: 1em; }
    .metric-block strong { color: #D33682; font-size: 1.2em; }
    .final-link a { color: #1E90FF; font-weight: bold; font-size: 1.1em; text-decoration: none; }
    .final-link a:hover { text-decoration: underline; }
</style>
""", unsafe_allow_html=True)

# --- CARGA DE DADOS INICIAL ---
initial_load_start = date.today() - timedelta(days=730)
initial_load_end = date.today()
df_pl_original = fetch_data_from_bigquery(
    project_gcp_id=BQ_PROJECT_ID_GCP, dataset_id=BQ_DATASET_ID, table_id=BQ_TABLE_LEADS,
    column_mapping=COLUNAS_BQ_LEADS_MAP, date_column_in_bq='Data',
    start_date_dt_func=initial_load_start, end_date_dt_func=initial_load_end,
)
df_p2_metas_original = carregar_planilha_gs(URL_PLANILHA_2, COLUNAS_PLANILHA_2_MAP, "data_meta_p2", "Planilha 2 (Metas)")

# --- INICIALIZAÇÃO CENTRALIZADA DO ESTADO DA SESSÃO (FILTROS) ---
if 'client_map_dict' not in st.session_state:
    client_map_dict = {}
    if not df_p2_metas_original.empty and 'cliente_p2' in df_p2_metas_original.columns and 'project_id_p2' in df_p2_metas_original.columns:
        client_project_map = df_p2_metas_original.dropna(subset=['cliente_p2', 'project_id_p2']).drop_duplicates(subset=['cliente_p2'])
        client_map_dict = pd.Series(client_project_map.project_id_p2.values, index=client_project_map.cliente_p2).to_dict()
    st.session_state.client_map_dict = client_map_dict
if 'selected_client' not in st.session_state:
    lista_clientes_inicial = sorted(list(st.session_state.client_map_dict.keys()))
    st.session_state.selected_client = lista_clientes_inicial[0] if lista_clientes_inicial else None
if 'start_date' not in st.session_state or 'end_date' not in st.session_state:
    today, yesterday = date.today(), date.today() - timedelta(days=1)
    if today.day == 1:
        start_date_default, end_date_default = yesterday.replace(day=1), yesterday
    else:
        start_date_default, end_date_default = today.replace(day=1), yesterday
    st.session_state.start_date, st.session_state.end_date = start_date_default, end_date_default
if 'conferidor_mode' not in st.session_state: st.session_state.conferidor_mode = False

# --- CHAMADA DA FUNÇÃO DE SIDEBAR ---
selected_client, selected_project_id, data_selecionada_inicio, data_selecionada_fim, conferidor_mode = render_persistent_sidebar()

# --- VALIDAÇÃO PÓS-SIDEBAR ---
if not selected_project_id:
    st.error("Por favor, selecione um cliente para carregar os dados.")
    st.stop()

# <--- CORREÇÃO: Adicionado bloco de validação para as datas
# Este bloco impede a execução do resto do script se uma das datas não for selecionada, evitando o TypeError.
if not data_selecionada_inicio or not data_selecionada_fim:
    st.warning("Por favor, selecione um período de data válido (início e fim) para continuar.")
    st.stop()

# --- CARREGAMENTO DE DADOS DO BIGQUERY (COM BASE NOS FILTROS) ---
df_google_kw_bq = fetch_data_from_bigquery(BQ_PROJECT_ID_GCP, BQ_DATASET_ID, BQ_TABLE_GKW, COLUNAS_BQ_KEYWORDS_MAP, 'date', data_selecionada_inicio, data_selecionada_fim, 'project_id', selected_project_id)
df_gads_bq = fetch_data_from_bigquery(BQ_PROJECT_ID_GCP, BQ_DATASET_ID, BQ_TABLE_GADS, COLUNAS_BQ_GADS_MAP, 'date', data_selecionada_inicio, data_selecionada_fim, 'project_id', selected_project_id)
df_fb_ads_bq = fetch_data_from_bigquery(BQ_PROJECT_ID_GCP, BQ_DATASET_ID, BQ_TABLE_FBADS, COLUNAS_BQ_FBADS_MAP, 'date_start', data_selecionada_inicio, data_selecionada_fim, 'project_id', selected_project_id)

# --- PROCESSAMENTO E CÁLCULOS ---
df_pl_filtrado_intervalo = pd.DataFrame()
if not df_pl_original.empty and "data_criacao" in df_pl_original.columns:
    df_pl_filtrado_projeto = df_pl_original[df_pl_original['project_id_pl'] == selected_project_id]
    mask_datas_pl1 = (df_pl_filtrado_projeto["data_criacao"].dt.date >= data_selecionada_inicio) & (df_pl_filtrado_projeto["data_criacao"].dt.date <= data_selecionada_fim)
    df_pl_filtrado_intervalo = df_pl_filtrado_projeto[mask_datas_pl1].copy()
df_pl1_conferidor = df_pl_filtrado_intervalo.copy()
df_p2_conferidor = pd.DataFrame()
if not df_p2_metas_original.empty and 'data_meta_p2' in df_p2_metas_original.columns:
    mask_p2 = (df_p2_metas_original['data_meta_p2'].dt.date >= data_selecionada_inicio) & (df_p2_metas_original['data_meta_p2'].dt.date <= data_selecionada_fim)
    df_p2_conferidor = df_p2_metas_original[mask_p2]

# --- CÁLCULO PERFORMANCE GERAL E KPIs ---
metricas_config = {"Faturamento": {"real_col_pl": "real_faturamento_venda_pl", "meta_col": "meta_faturamento_p2", "acumulada": True, "formato": "R$ {:,.2f}", "regra_status": {"ruim_ate": 74.99, "atc_ate": 99.99, "bom_acima": 100, "tipo": "maior_melhor"}}, "Investimento": {"meta_col": "meta_investimento_p2", "acumulada": True, "formato": "R$ {:,.2f}", "regra_status": {"bom_entre": (95, 105), "atc_entre_inf": (85, 94.99), "atc_entre_sup": (105.01, 115), "ruim_fora": True, "tipo": "dentro_faixa_invest"}}, "Leads": {"real_pl_count": True, "meta_col": "meta_leads_p2", "acumulada": True, "formato": "{:,.0f}", "regra_status": {"ruim_ate": 79.99, "atc_ate": 95.99, "bom_acima": 96, "tipo": "maior_melhor"}}, "MQL": {"real_pl_qualificacao": "MQL", "meta_col": "meta_mql_p2", "acumulada": True, "formato": "{:,.0f}", "regra_status": {"ruim_ate": 79.99, "atc_ate": 95.99, "bom_acima": 96, "tipo": "maior_melhor"}}, "Vendas": {"real_pl_count_faturamento": True, "meta_col": "meta_vendas_p2", "acumulada": True, "formato": "{:,.0f}", "regra_status": {"ruim_ate": 79.99, "atc_ate": 95.99, "bom_acima": 96, "tipo": "maior_melhor"}}, "Ticket Médio": {"real_calc": "Faturamento/Vendas", "meta_col": "meta_ticket_medio_p2", "acumulada": False, "formato": "R$ {:,.2f}", "regra_status": {"ruim_ate": 74.99, "atc_ate": 89.99, "bom_acima": 90, "tipo": "maior_melhor"}}, "Taxa de MQL": {"real_calc": "MQL/Leads", "meta_col": "meta_conv_lead_mql_p2", "acumulada": False, "formato": "{:.1f}%", "regra_status": {"bom_entre": (95, 105), "atc_entre_inf": (85, 94.99), "atc_entre_sup": (105.01, 115), "ruim_fora": True, "tipo": "dentro_faixa_percentual"}}, "CPL": {"real_calc": "Investimento/Leads", "meta_col": "meta_cpl_p2", "acumulada": False, "formato": "R$ {:,.2f}", "regra_status": {"ruim_fora": True, "tipo": "custo_menor_melhor"}}, "CPMQL": {"real_calc": "Investimento/MQL", "meta_col": "meta_cpmql_p2", "acumulada": False, "formato": "R$ {:,.2f}", "regra_status": {"ruim_fora": True, "tipo": "custo_menor_melhor"}}, "ROAS": {"real_calc": "Faturamento/Investimento", "meta_col": "meta_roas_p2", "acumulada": False, "formato": "{:,.2f}x", "regra_status": {"tipo": "valor_absoluto", "ruim_max": 4, "saudavel_max": 10}}, "ROI": {"real_calc": "(Faturamento * LT * MC) / Investimento", "meta_col": "meta_roi_p2", "acumulada": False, "formato": "{:,.2f}x", "regra_status": {"tipo": "valor_absoluto", "ruim_max": 1, "saudavel_max": 3}}, "Growth Rate": {"real_calc": "((Faturamento * MC) - Investimento) / Fee", "meta_col": "meta_growth_rate_p2", "acumulada": False, "formato": "{:,.2f}", "regra_status": {"tipo": "valor_absoluto", "ruim_max": 1, "atencao_max": 2}}}
linhas_tabela_performance = ["Realizado", "Meta", "Diferença", "% Atingido", "Status"]
df_tabela_performance = pd.DataFrame(index=linhas_tabela_performance, columns=metricas_config.keys()).astype(object)
df_tabela_performance.loc[["Realizado", "Meta", "Diferença", "% Atingido"]] = 0.0
df_tabela_performance.loc["Status"] = ""
realizados_calculados = {}
if not df_pl_filtrado_intervalo.empty:
    realizados_calculados["Faturamento"] = df_pl_filtrado_intervalo['real_faturamento_venda_pl'].sum() if 'real_faturamento_venda_pl' in df_pl_filtrado_intervalo.columns else 0
    realizados_calculados["Leads"] = len(df_pl_filtrado_intervalo)
    realizados_calculados["MQL"] = len(df_pl_filtrado_intervalo[df_pl_filtrado_intervalo['qualificacao_pl'] == "MQL"]) if 'qualificacao_pl' in df_pl_filtrado_intervalo.columns else 0
    realizados_calculados["Vendas"] = len(df_pl_filtrado_intervalo[df_pl_filtrado_intervalo['real_faturamento_venda_pl'] > 0]) if 'real_faturamento_venda_pl' in df_pl_filtrado_intervalo.columns else 0
else:
    for m_key_pl in ["Faturamento", "Leads", "MQL", "Vendas"]: realizados_calculados[m_key_pl] = 0.0
investimento_total = 0.0
if not df_gads_bq.empty and 'cost_gads' in df_gads_bq.columns: investimento_total += df_gads_bq['cost_gads'].sum()
if not df_fb_ads_bq.empty and 'Investido_fb_val' in df_fb_ads_bq.columns: investimento_total += df_fb_ads_bq['Investido_fb_val'].sum()
realizados_calculados["Investimento"] = investimento_total
lt_valor, mc_valor, fee_valor = 0.0, 0.0, 0.0
if not df_p2_metas_original.empty and 'project_id_p2' in df_p2_metas_original.columns:
    metas_projeto_df = df_p2_metas_original[df_p2_metas_original['project_id_p2'] == selected_project_id]
    if not metas_projeto_df.empty:
        ultimo_mes_filtro = pd.Period(data_selecionada_fim, freq='M')
        meta_mes_especifico_df = metas_projeto_df[metas_projeto_df['ano_mes'] == ultimo_mes_filtro]
        if not meta_mes_especifico_df.empty:
            lt_valor = meta_mes_especifico_df['lt_p2'].iloc[0] if 'lt_p2' in meta_mes_especifico_df.columns else 0.0
            mc_valor = meta_mes_especifico_df['mc_p2'].iloc[0] if 'mc_p2' in meta_mes_especifico_df.columns else 0.0
            fee_valor = meta_mes_especifico_df['fee_p2'].iloc[0] if 'fee_p2' in meta_mes_especifico_df.columns else 0.0
realizados_calculados["Ticket Médio"] = (realizados_calculados["Faturamento"] / realizados_calculados["Vendas"]) if realizados_calculados["Vendas"] > 0 else 0.0
realizados_calculados["Taxa de MQL"] = (realizados_calculados["MQL"] / realizados_calculados["Leads"] * 100) if realizados_calculados["Leads"] > 0 else 0.0
realizados_calculados["CPL"] = (realizados_calculados["Investimento"] / realizados_calculados["Leads"]) if realizados_calculados["Leads"] > 0 else 0.0
realizados_calculados["CPMQL"] = (realizados_calculados["Investimento"] / realizados_calculados["MQL"]) if realizados_calculados["MQL"] > 0 else 0.0
faturamento_real, investimento_real = realizados_calculados["Faturamento"], realizados_calculados["Investimento"]
realizados_calculados["ROAS"] = faturamento_real / investimento_real if investimento_real > 0 else 0.0
realizados_calculados["ROI"] = (faturamento_real * lt_valor * mc_valor) / investimento_real if investimento_real > 0 else 0.0
realizados_calculados["Growth Rate"] = ((faturamento_real * mc_valor) - investimento_real) / fee_valor if fee_valor > 0 else 0.0
for metrica, valor_real in realizados_calculados.items():
    if metrica in df_tabela_performance.columns: df_tabela_performance.loc["Realizado", metrica] = valor_real
for metrica_nome, config in metricas_config.items():
    meta_nome, valor_meta_periodo = config.get("meta_col"), 0.0
    if meta_nome and not df_p2_metas_original.empty and meta_nome in df_p2_metas_original.columns and 'project_id_p2' in df_p2_metas_original.columns and 'ano_mes' in df_p2_metas_original.columns:
        metas_projeto_df = df_p2_metas_original[df_p2_metas_original['project_id_p2'] == selected_project_id]
        if not metas_projeto_df.empty:
            if config["acumulada"]:
                soma_meta_pro_rata = 0.0
                for dia_obj in pd.date_range(start=data_selecionada_inicio, end=data_selecionada_fim, freq='D'):
                    ano_mes_corrente = pd.Period(dia_obj, freq='M')
                    meta_mensal_df = metas_projeto_df[metas_projeto_df['ano_mes'] == ano_mes_corrente]
                    if not meta_mensal_df.empty:
                        meta_mensal_valor = meta_mensal_df[meta_nome].iloc[0]
                        dias_no_mes_meta = calendar.monthrange(ano_mes_corrente.year, ano_mes_corrente.month)[1]
                        meta_diaria = meta_mensal_valor / dias_no_mes_meta if dias_no_mes_meta > 0 else 0
                        soma_meta_pro_rata += meta_diaria
                valor_meta_periodo = soma_meta_pro_rata
            else:
                ultimo_mes_filtro = pd.Period(data_selecionada_fim, freq='M')
                meta_mes_especifico_df = metas_projeto_df[metas_projeto_df['ano_mes'] == ultimo_mes_filtro]
                if not meta_mes_especifico_df.empty: valor_meta_periodo = meta_mes_especifico_df[meta_nome].iloc[0]
    df_tabela_performance.loc["Meta", metrica_nome] = valor_meta_periodo
    real, meta = df_tabela_performance.loc["Realizado", metrica_nome], df_tabela_performance.loc["Meta", metrica_nome]
    df_tabela_performance.loc["Diferença", metrica_nome] = real - meta
    percent_atingido, tipo_regra = 0.0, config.get("regra_status", {}).get("tipo", "maior_melhor")
    if tipo_regra == "custo_menor_melhor":
        if real > 0: percent_atingido = (meta / real) * 100
        elif real == 0 and meta > 0: percent_atingido = float('inf')
        elif real == 0 and meta == 0: percent_atingido = 100
    else:
        if meta > 0: percent_atingido = (real / meta) * 100
        elif real > 0 and meta == 0: percent_atingido = float('inf')
        elif real == 0 and meta == 0: percent_atingido = 100
    if metrica_nome == "Taxa de MQL":
        meta_em_percentual, real_percentual = meta * 100, real
        if meta_em_percentual > 0: percent_atingido = (real_percentual / meta_em_percentual) * 100
        elif real_percentual > 0 and meta_em_percentual == 0: percent_atingido = float('inf')
        elif real_percentual == 0 and meta_em_percentual == 0: percent_atingido = 100
    df_tabela_performance.loc["% Atingido", metrica_nome] = percent_atingido

# --- ESTRUTURA PRINCIPAL DO DASHBOARD ---
payload_data_checkin = {"cliente": selected_client, "periodo_inicio": data_selecionada_inicio.isoformat(), "periodo_fim": data_selecionada_fim.isoformat(), "metricas_gerais": {}, "kpis_google_ads": {}, "kpis_meta_ads": {}}
metricas_para_exibir_checkin = ["Faturamento", "Investimento", "Leads", "MQL", "Vendas", "Ticket Médio", "CPL", "CPMQL", "Taxa de MQL", "ROAS", "ROI", "Growth Rate"]
for metrica in metricas_para_exibir_checkin:
    if metrica in df_tabela_performance.columns:
        realizado, meta, diferenca, atingido_perc, status = df_tabela_performance.loc[:, metrica]
        chave_payload = metrica.lower().replace(" ", "_").replace("→", "to").replace("%", "perc").replace(">", "")
        payload_data_checkin["metricas_gerais"][chave_payload] = {"realizado": realizado, "meta": meta, "diferenca": diferenca, "atingido_perc": atingido_perc, "status": status}
pl_cols_ok_for_google = not df_pl_filtrado_intervalo.empty and all(c in df_pl_filtrado_intervalo.columns for c in ['utm_term_pl', 'utm_campaign_pl', 'qualificacao_pl', 'real_faturamento_venda_pl'])
agg_pl = {'Leads': pd.NamedAgg(column='project_id_pl', aggfunc='count'), 'MQL': pd.NamedAgg(column='qualificacao_pl', aggfunc=lambda x: (x == 'MQL').sum()), 'Vendas': pd.NamedAgg(column='real_faturamento_venda_pl', aggfunc=lambda x: (x > 0).sum()), 'Valor': pd.NamedAgg(column='real_faturamento_venda_pl', aggfunc='sum')}
df_pl_processed_kw, df_pl_processed_gads, pl_agg_for_fb = pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
if pl_cols_ok_for_google:
    df_pl_google_base = df_pl_filtrado_intervalo.copy()
    df_pl_processed_kw = df_pl_google_base.groupby('utm_term_pl').agg(**agg_pl).reset_index()
    if 'utm_campaign_pl' in df_pl_google_base.columns: df_pl_processed_gads = df_pl_google_base.groupby('utm_campaign_pl').agg(**agg_pl).reset_index()
    pl_agg_for_fb = df_pl_filtrado_intervalo.groupby('utm_term_pl').agg(**agg_pl).reset_index()
tabela_gads = pd.DataFrame()
try:
    if not df_gads_bq.empty and 'campaign_id_gads' in df_gads_bq.columns:
        df_gads_agg = df_gads_bq.groupby(['campaign_name_gads', 'campaign_id_gads']).agg(Investido=('cost_gads', 'sum'), Impressões=('impressions_gads', 'sum'), Cliques=('clicks_gads', 'sum')).reset_index()
        if not df_pl_processed_gads.empty: tabela_gads = pd.merge(df_gads_agg, df_pl_processed_gads, left_on='campaign_id_gads', right_on='utm_campaign_pl', how='left')
        else: tabela_gads = df_gads_agg.copy()
        numeric_cols_to_fill = ['Leads', 'MQL', 'Vendas', 'Valor']
        for col in numeric_cols_to_fill:
            if col not in tabela_gads.columns: tabela_gads[col] = 0
            tabela_gads[col] = pd.to_numeric(tabela_gads[col], errors='coerce').fillna(0)
        tabela_gads['CTR'] = (tabela_gads['Cliques'] / tabela_gads['Impressões'] * 100).fillna(0)
        tabela_gads['Taxa de conversão'] = (tabela_gads['Leads'] / tabela_gads['Cliques'] * 100).fillna(0)
        tabela_gads['% MQL'] = (tabela_gads['MQL'] / tabela_gads['Leads'] * 100).fillna(0)
        tabela_gads['CPL'] = (tabela_gads['Investido'] / tabela_gads['Leads']).fillna(0)
        tabela_gads['CPMQL'] = (tabela_gads['Investido'] / tabela_gads['MQL']).fillna(0)
        tabela_gads.replace([np.inf, -np.inf], 0.0, inplace=True)
        tabela_gads.rename(columns={'campaign_name_gads': 'Campanha'}, inplace=True)
        if not tabela_gads.empty:
            investido_gads_total, leads_gads_total, mql_gads_total, total_cliques_gads, total_impressoes_gads = tabela_gads['Investido'].sum(), tabela_gads['Leads'].sum(), tabela_gads['MQL'].sum(), tabela_gads['Cliques'].sum(), tabela_gads['Impressões'].sum()
            payload_data_checkin['kpis_google_ads'] = {'total_investido': investido_gads_total, 'total_leads': leads_gads_total, 'total_mql': mql_gads_total, 'total_vendas': tabela_gads['Vendas'].sum(), 'valor_total': tabela_gads['Valor'].sum(), 'ctr_medio': (total_cliques_gads / total_impressoes_gads * 100) if total_impressoes_gads > 0 else 0, 'taxa_conv_media': (leads_gads_total / total_cliques_gads * 100) if total_cliques_gads > 0 else 0, 'perc_mql': (mql_gads_total / leads_gads_total * 100) if leads_gads_total > 0 else 0, 'total_impressoes': total_impressoes_gads, 'total_cliques': total_cliques_gads}
except Exception as e:
    st.warning(f"Não foi possível processar os dados da tabela do Google Ads. Erro: {e}")
    tabela_gads = pd.DataFrame()
df_fb_creatives_final = pd.DataFrame()
try:
    if not df_fb_ads_bq.empty:
        fb_ads_grouped_period = df_fb_ads_bq.groupby(['project_id_fb', 'ad_id_fb', 'Criativo_fb_ad_name', 'adset_name_fb', 'campaign_name_fb'], as_index=False).agg(Investido=('Investido_fb_val', 'sum'), Impressões=('Impressões_fb_val', 'sum'), Cliques=('Cliques_fb_val', 'sum'))
        merged_fb_tahap_2 = fb_ads_grouped_period.copy()
        if not pl_agg_for_fb.empty:
            merged_fb_tahap_2 = pd.merge(merged_fb_tahap_2, pl_agg_for_fb, left_on='ad_id_fb', right_on='utm_term_pl', how='left')
            merged_fb_tahap_2.drop(columns=['utm_term_pl'], inplace=True, errors='ignore')
        numeric_cols_to_fill_fb = ['Leads', 'MQL', 'Vendas', 'Valor']
        for col in numeric_cols_to_fill_fb:
            if col not in merged_fb_tahap_2.columns: merged_fb_tahap_2[col] = 0
            merged_fb_tahap_2[col] = pd.to_numeric(merged_fb_tahap_2[col], errors='coerce').fillna(0)
        df_fb_creatives_final = merged_fb_tahap_2.copy()
        df_fb_creatives_final.rename(columns={'Criativo_fb_ad_name': 'Criativo', 'adset_name_fb': 'Público', 'campaign_name_fb': 'Campanha'}, inplace=True)
        df_fb_creatives_final['CTR'] = (df_fb_creatives_final['Cliques'] / df_fb_creatives_final['Impressões'] * 100).fillna(0)
        df_fb_creatives_final['Taxa de conversão'] = (df_fb_creatives_final['Leads'] / df_fb_creatives_final['Cliques'] * 100).fillna(0)
        df_fb_creatives_final['% MQL'] = (df_fb_creatives_final['MQL'] / df_fb_creatives_final['Leads'] * 100).fillna(0)
        df_fb_creatives_final['CPL'] = (df_fb_creatives_final['Investido'] / df_fb_creatives_final['Leads']).fillna(0)
        df_fb_creatives_final['CPMQL'] = (df_fb_creatives_final['Investido'] / df_fb_creatives_final['MQL']).fillna(0)
        df_fb_creatives_final.replace([np.inf, -np.inf], 0.0, inplace=True)
        if not df_fb_creatives_final.empty:
            df_meta_ads_agg_kpi = df_fb_creatives_final.groupby('Campanha').agg(Impressões=('Impressões', 'sum'), Cliques=('Cliques', 'sum'), Leads=('Leads', 'sum'), MQL=('MQL', 'sum'), Investido=('Investido', 'sum'), Vendas=('Vendas', 'sum'), Valor=('Valor', 'sum')).reset_index()
            investido_fb_total, leads_fb_total, mql_fb_total, total_cliques_fb, total_impressoes_fb = df_meta_ads_agg_kpi['Investido'].sum(), df_meta_ads_agg_kpi['Leads'].sum(), df_meta_ads_agg_kpi['MQL'].sum(), df_meta_ads_agg_kpi['Cliques'].sum(), df_meta_ads_agg_kpi['Impressões'].sum()
            payload_data_checkin['kpis_meta_ads'] = {'total_investido': investido_fb_total, 'total_leads': leads_fb_total, 'total_mql': mql_fb_total, 'total_vendas': df_meta_ads_agg_kpi['Vendas'].sum(), 'valor_total': df_meta_ads_agg_kpi['Valor'].sum(), 'ctr_medio': (total_cliques_fb / total_impressoes_fb * 100) if total_impressoes_fb > 0 else 0, 'taxa_conv_media': (leads_fb_total / total_cliques_fb * 100) if total_cliques_fb > 0 else 0, 'perc_mql': (mql_fb_total / leads_fb_total * 100) if leads_fb_total > 0 else 0, 'total_impressoes': total_impressoes_fb, 'total_cliques': total_cliques_fb}
except Exception as e:
    st.warning(f"Não foi possível processar os dados da tabela do Meta Ads. Erro: {e}")
    df_fb_creatives_final = pd.DataFrame()
def trigger_webhook(webhook_url, payload, button_name, success_title):
    if not webhook_url:
        st.error(f"A URL do Webhook para '{button_name}' não foi configurada nos segredos do Streamlit.")
        return
    cleaned_payload = clean_and_round_payload(payload)
    with st.spinner(f"Criando sua solicitação de '{button_name}'. Aguarde..."):
        try:
            headers = {'Content-Type': 'application/json'}
            response = requests.post(webhook_url, data=json.dumps(cleaned_payload, allow_nan=False), headers=headers, timeout=180)
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    presentation_link = response_data.get("presentation_url")
                    if presentation_link:
                        st.balloons()
                        st.session_state.show_success_dialog, st.session_state.presentation_link, st.session_state.success_title = True, presentation_link, success_title
                        st.rerun()
                    else:
                        st.error("Resposta recebida, mas o link da apresentação não foi encontrado.")
                        st.json(response_data)
                except json.JSONDecodeError:
                    st.error("A automação funcionou, mas a resposta não foi um JSON válido."); st.text(response.text)
            else:
                st.error(f"Erro ao comunicar com o webhook: {response.status_code}"); st.text(response.text)
        except requests.exceptions.Timeout: st.error("A requisição demorou muito para responder (timeout).")
        except requests.exceptions.RequestException as e: st.error(f"Erro de conexão ao tentar enviar o webhook: {e}")
title_col, btn_col1, btn_col2, btn_col3 = st.columns([2, 1, 1, 1])
with title_col:
    if selected_client: st.markdown(f"<h1 style='text-align: left; margin-left: 20px;'>{selected_client}</h1>", unsafe_allow_html=True)
    else: st.markdown("<h1 style='text-align: left; margin-left: 20px;'>Dashboard</h1>", unsafe_allow_html=True)
with btn_col1:
    st.write(""), st.write("")
    if st.button("Check-in", key="btn1"): trigger_webhook(WEBHOOK_CHECKIN, payload_data_checkin, "Check-in", "Concluído!")
with btn_col2:
    st.write(""), st.write("")
    if st.button("Relatório", key="btn2"): trigger_webhook(WEBHOOK_RELATORIO, payload_data_checkin, "Relatório", "Concluído!")
with btn_col3:
    st.write(""), st.write("")
    if st.button("Hipóteses", key="btn3"): trigger_webhook(WEBHOOK_HIPOTESES, payload_data_checkin, "Hipóteses", "Concluído!")
if st.session_state.get("show_success_dialog", False):
    dialog_title = st.session_state.get("success_title", "Concluído!")
    @st.dialog(dialog_title)
    def show_link_dialog():
        st.markdown("Clique no link para ver o resultado:")
        link = st.session_state.get("presentation_link", "")
        st.markdown(f'<p class="final-link"><a href="{link}" target="_blank">Visualizar</a></p>', unsafe_allow_html=True)
        if st.button("Fechar", key="close_dialog"):
            st.session_state.show_success_dialog, st.session_state.presentation_link, st.session_state.success_title = False, "", ""
            st.rerun()
    show_link_dialog()
st.markdown(HR_SEPARATOR_STYLE, unsafe_allow_html=True)

def get_status_by_value(metric_name, real_value, regra_status):
    if pd.isna(real_value) or real_value == float('inf') or real_value == float('-inf'): return "N/A", ""
    if metric_name == "ROI":
        if real_value < regra_status.get("ruim_max", 1): return "Ruim", f'background-color: {CORES_STATUS_TEXTO["Ruim"]}'
        elif real_value <= regra_status.get("saudavel_max", 3): return "Saudável", f'background-color: {CORES_STATUS_TEXTO["Saudável"]}'
        else: return "Atenção", f'background-color: {CORES_STATUS_TEXTO["Atenção"]}'
    elif metric_name == "ROAS":
        if real_value < regra_status.get("ruim_max", 4): return "Ruim", f'background-color: {CORES_STATUS_TEXTO["Ruim"]}'
        elif real_value <= regra_status.get("saudavel_max", 10): return "Saudável", f'background-color: {CORES_STATUS_TEXTO["Saudável"]}'
        else: return "Atenção", f'background-color: {CORES_STATUS_TEXTO["Atenção"]}'
    elif metric_name == "Growth Rate":
        if real_value < regra_status.get("ruim_max", 1): return "Ruim", f'background-color: {CORES_STATUS_TEXTO["Ruim"]}'
        elif real_value <= regra_status.get("atencao_max", 2): return "Atenção", f'background-color: {CORES_STATUS_TEXTO["Atenção"]}'
        else: return "Saudável", f'background-color: {CORES_STATUS_TEXTO["Saudável"]}'
    return "N/A", ""
def get_status_by_percent(percent_value, regra_status_config):
    status_texto, cor_de_fundo_css, tipo = "N/A", "", regra_status_config.get("tipo")
    if pd.isna(percent_value) or tipo is None or percent_value == float('inf') or percent_value == float('-inf'):
        if percent_value == float('inf') and tipo != "custo_menor_melhor": return "Saudável", f'background-color: {CORES_STATUS_TEXTO["Saudável"]}'
        elif percent_value == float('inf') and tipo == "custo_menor_melhor": return "Ruim", f'background-color: {CORES_STATUS_TEXTO["Ruim"]}'
        elif percent_value == float('-inf'): return "Ruim", f'background-color: {CORES_STATUS_TEXTO["Ruim"]}'
        return status_texto, cor_de_fundo_css
    if tipo == "maior_melhor":
        if percent_value >= regra_status_config.get("bom_acima", 100): status_texto, cor_de_fundo_css = "Saudável", f'background-color: {CORES_STATUS_TEXTO["Saudável"]}'
        elif percent_value > regra_status_config.get("ruim_ate", 80): status_texto, cor_de_fundo_css = "Atenção", f'background-color: {CORES_STATUS_TEXTO["Atenção"]}'
        else: status_texto, cor_de_fundo_css = "Ruim", f'background-color: {CORES_STATUS_TEXTO["Ruim"]}'
    elif tipo == "custo_menor_melhor":
        if percent_value >= 100: status_texto, cor_de_fundo_css = "Saudável", f'background-color: {CORES_STATUS_TEXTO["Saudável"]}'
        elif percent_value >= 90: status_texto, cor_de_fundo_css = "Atenção", f'background-color: {CORES_STATUS_TEXTO["Atenção"]}'
        else: status_texto, cor_de_fundo_css = "Ruim", f'background-color: {CORES_STATUS_TEXTO["Ruim"]}'
    elif tipo in ["dentro_faixa_invest", "dentro_faixa_percentual"]:
        bom_min, bom_max = regra_status_config.get("bom_entre", (95, 105))
        atc_inf_min, atc_inf_max = regra_status_config.get("atc_entre_inf", (85, bom_min - 0.01))
        atc_sup_min, atc_sup_max = regra_status_config.get("atc_entre_sup", (bom_max + 0.01, 115))
        if bom_min <= percent_value <= bom_max: status_texto, cor_de_fundo_css = "Saudável", f'background-color: {CORES_STATUS_TEXTO["Saudável"]}'
        elif (atc_inf_min <= percent_value <= atc_inf_max) or (atc_sup_min <= percent_value <= atc_sup_max): status_texto, cor_de_fundo_css = "Atenção", f'background-color: {CORES_STATUS_TEXTO["Atenção"]}'
        else: status_texto, cor_de_fundo_css = "Ruim", f'background-color: {CORES_STATUS_TEXTO["Ruim"]}'
    return status_texto, cor_de_fundo_css
for metrica_col in df_tabela_performance.columns:
    regra_cfg = metricas_config[metrica_col].get("regra_status", {})
    tipo_regra = regra_cfg.get("tipo")
    status_txt = "N/A"
    if tipo_regra == 'valor_absoluto':
        real_val = df_tabela_performance.loc["Realizado", metrica_col]
        status_txt, _ = get_status_by_value(metrica_col, real_val, regra_cfg)
    else:
        percent_val = df_tabela_performance.loc["% Atingido", metrica_col]
        status_txt, _ = get_status_by_percent(percent_val, regra_cfg)
    df_tabela_performance.loc["Status", metrica_col] = status_txt

# --- SEÇÃO DE KPIs ATUALIZADA E RESPONSIVA ---
st.markdown("<h2 style='text-align: center;'>Performance Geral do Projeto</h2>", unsafe_allow_html=True)
kpi_order = ["Investimento", "Faturamento", "ROAS", "ROI", "Vendas", "Ticket Médio", "Leads", "CPL", "Taxa de MQL", "Growth Rate", "MQL", "CPMQL"]

cols = st.columns(4)
for i, metrica in enumerate(kpi_order):
    with cols[i % 4]:
        if metrica not in df_tabela_performance.columns:
            st.markdown(f"""
            <div class="performance-kpi-card metric-card-na">
                <div class="kpi-header"><span class="kpi-metric-name">{metrica}</span></div>
                <div>Dados Indisponíveis</div>
            </div>""", unsafe_allow_html=True)
            continue
        realizado, meta, diferenca, percent_atingido, status = df_tabela_performance.loc[:, metrica]
        if status == 'N/A': status_class = 'na'
        elif isinstance(status, str) and status: status_class = status.lower().replace('á', 'a').replace('ç', 'c')
        else: status_class = 'na'
        valid_classes = ['saudavel', 'atencao', 'ruim', 'na']
        if status_class not in valid_classes: status_class = 'na'
        formato_valor = metricas_config[metrica]['formato']
        realizado_fmt = format_brazilian(realizado, formato_valor)
        if metrica == "Taxa de MQL":
            meta_fmt = format_brazilian(meta * 100, "{:,.1f}%")
            diferenca_fmt = f"{format_brazilian(diferenca, '{:,.1f}')} p.p."
        else:
            meta_fmt = format_brazilian(meta, formato_valor)
            diferenca_fmt = format_brazilian(diferenca, formato_valor)
        atingido_fmt = f"{format_brazilian(percent_atingido, '{:,.1f}')}%"
        if pd.isna(percent_atingido) or percent_atingido == float('inf') or percent_atingido == float('-inf'): atingido_fmt = "N/A"
        help_text = "descrição"
        if metrica == "ROI": help_text = f"ROI considerando que o LT seja {format_brazilian(lt_valor, '{:,.2f}')}"
        kpi_html = f"""
        <div class="performance-kpi-card metric-card-{status_class}">
            <div>
                <div class="kpi-header">
                    <span class="kpi-metric-name">{metrica}</span>
                    <span class="kpi-help-icon" title="{help_text}">?</span>
                </div>
                <div class="kpi-main-value">{realizado_fmt}</div>
            </div>
            <div>
                <div class="kpi-details-grid">
                    <span>Meta: {meta_fmt}</span><br>
                    <span>Diferença: {diferenca_fmt}</span>
                </div>
                <div class="kpi-percent-achieved">{atingido_fmt}</div>
            </div>
        </div>
        """
        st.markdown(kpi_html, unsafe_allow_html=True)

st.markdown(HR_SEPARATOR_STYLE, unsafe_allow_html=True)

# O restante do código permanece inalterado...
st.markdown("<h2 style='text-align: center;'>Leads e MQLs Gerados</h2>", unsafe_allow_html=True)
granularity_options = ["Dia a dia", "Semanal", "Mensal"]
selected_granularity = st.radio("Visualizar por:", granularity_options, horizontal=True, index=0, key="leads_granularity_selector")
if not df_pl_filtrado_intervalo.empty and "data_criacao" in df_pl_filtrado_intervalo.columns and "qualificacao_pl" in df_pl_filtrado_intervalo.columns:
    df_chart_source = df_pl_filtrado_intervalo.copy()
    df_chart_source['data_original'] = pd.to_datetime(df_chart_source['data_criacao'].dt.date)
    grouping_col = 'Periodo'
    if selected_granularity == "Dia a dia":
        df_chart_source[grouping_col] = df_chart_source['data_original']
        all_dates_for_range = pd.DataFrame({grouping_col: pd.date_range(start=data_selecionada_inicio, end=data_selecionada_fim, freq='D')})
    elif selected_granularity == "Semanal":
        df_chart_source[grouping_col] = df_chart_source['data_original'].apply(lambda x: x - pd.to_timedelta(x.weekday(), unit='D'))
        start_week = (pd.to_datetime(data_selecionada_inicio) - pd.to_timedelta(pd.to_datetime(data_selecionada_inicio).weekday(), unit='D')).normalize()
        all_dates_for_range = pd.DataFrame({grouping_col: pd.date_range(start=start_week, end=data_selecionada_fim, freq='W-MON')})
    elif selected_granularity == "Mensal":
        df_chart_source[grouping_col] = df_chart_source['data_original'].apply(lambda x: x.replace(day=1))
        start_month = pd.to_datetime(data_selecionada_inicio).replace(day=1)
        all_dates_for_range = pd.DataFrame({grouping_col: pd.date_range(start=start_month, end=data_selecionada_fim, freq='MS')})
    all_dates_for_range[grouping_col] = pd.to_datetime(all_dates_for_range[grouping_col])
    mqls_agg = df_chart_source[df_chart_source['qualificacao_pl'] == 'MQL'].groupby(grouping_col).size().reset_index(name='MQL_Count')
    if not mqls_agg.empty: mqls_agg[grouping_col] = pd.to_datetime(mqls_agg[grouping_col])
    else: mqls_agg = pd.DataFrame({grouping_col: pd.to_datetime([]), 'MQL_Count': []})
    leads_qual_agg = df_chart_source[df_chart_source['qualificacao_pl'] == 'Lead'].groupby(grouping_col).size().reset_index(name='Lead_Qual_Count')
    if not leads_qual_agg.empty: leads_qual_agg[grouping_col] = pd.to_datetime(leads_qual_agg[grouping_col])
    else: leads_qual_agg = pd.DataFrame({grouping_col: pd.to_datetime([]), 'Lead_Qual_Count': []})
    processed_counts = pd.merge(all_dates_for_range, mqls_agg, on=grouping_col, how='left')
    processed_counts = pd.merge(processed_counts, leads_qual_agg, on=grouping_col, how='left')
    processed_counts[['MQL_Count', 'Lead_Qual_Count']] = processed_counts[['MQL_Count', 'Lead_Qual_Count']].fillna(0).astype(int)
    df_leads_bar, df_mql_bar = processed_counts[[grouping_col]].copy(), processed_counts[[grouping_col]].copy()
    df_leads_bar['Count'], df_leads_bar['Tipo'] = processed_counts['Lead_Qual_Count'], 'Lead'
    df_mql_bar['Count'], df_mql_bar['Tipo'] = processed_counts['MQL_Count'], 'MQL'
    grafico_df_completo = pd.concat([df_mql_bar, df_leads_bar])
    grafico_df_completo.sort_values(by=[grouping_col, 'Tipo'], ascending=[True, False], inplace=True)
    if selected_granularity == "Dia a dia":
        day_map_pt = {'Mon': 'seg', 'Tue': 'ter', 'Wed': 'qua', 'Thu': 'qui', 'Fri': 'sex', 'Sat': 'sáb', 'Sun': 'dom'}
        grafico_df_completo['day_abbr_en'] = grafico_df_completo[grouping_col].dt.strftime('%a')
        grafico_df_completo['DisplayLabel'] = grafico_df_completo['day_abbr_en'].map(day_map_pt).fillna(grafico_df_completo['day_abbr_en']) + '<br>' + grafico_df_completo[grouping_col].dt.strftime('%d/%m')
    elif selected_granularity == "Semanal":
        grafico_df_completo['DisplayLabel'] = grafico_df_completo[grouping_col].dt.strftime('Sem %U<br>(%d/%m)')
    elif selected_granularity == "Mensal":
        grafico_df_completo['DisplayLabel'] = grafico_df_completo[grouping_col].dt.strftime('%b/%Y')
    unique_ordered_periods = grafico_df_completo[[grouping_col, 'DisplayLabel']].drop_duplicates().sort_values(by=grouping_col)
    sorted_x_labels = unique_ordered_periods['DisplayLabel'].tolist()
    if not grafico_df_completo.empty:
        fig_leads_dia = px.bar(grafico_df_completo, x="DisplayLabel", y="Count", color="Tipo", labels={"DisplayLabel": "Período", "Count": "Quantidade"}, color_discrete_map={'Lead': '#FFFFFF', 'MQL': '#28a745'}, category_orders={'Tipo': ['MQL', 'Lead'], 'DisplayLabel': sorted_x_labels})
        fig_leads_dia.update_layout(plot_bgcolor='rgba(42,42,42,1)', paper_bgcolor='rgba(0,0,0,0)', legend_title_text='', yaxis=dict(tickfont=dict(color='white'), title=dict(font=dict(color='white'))), xaxis=dict(tickfont=dict(color='white'), title=dict(font=dict(color='white'))), legend=dict(font=dict(color='white')))
        fig_leads_dia.update_yaxes(tickformat="d")
        st.plotly_chart(fig_leads_dia, use_container_width=True)
    else:
        st.info("Não há dados de Leads para exibir no gráfico para o período e granularidade selecionados.")
else:
    st.info("Dados de Leads (BigQuery) não disponíveis ou incompletos para gerar o gráfico.")

st.markdown("<h2 style='text-align: center;'>Fontes de Tráfego</h2>", unsafe_allow_html=True)
if not df_pl_filtrado_intervalo.empty and "utm_source_pl" in df_pl_filtrado_intervalo.columns:
    df_source_chart = df_pl_filtrado_intervalo.copy()
    source_map = {'fb': 'Facebook', 'ig': 'Instagram', 'an': 'Audience', 'bio': 'Bio', 'g': 'Search', 's': 'Partners', 'd': 'Display', 'ytv': 'Youtube Video', 'yt': 'Youtube', 't': 'Video Partners'}
    color_map = {'Facebook': '#1877F2', 'Instagram': "#F29B18", 'Audience': "#8814D1", 'Bio': "#18CEF2", 'Search': '#34A853', 'Partners': "#D729C0", 'Display': "#EDEA18", 'Youtube Video': "#F30C0C", 'Youtube': '#F30C0C', 'Video Partners': "#885D18", 'Não identificado': "#7E7E7E", 'Nulo': "#B6B6B6"}
    df_source_chart['source_mapped'] = df_source_chart['utm_source_pl'].map(source_map).fillna(df_source_chart['utm_source_pl'].apply(lambda x: 'Nulo' if pd.isna(x) or str(x).lower() in ['nan', '<na>'] else 'Não identificado'))
    source_agg = df_source_chart.groupby('source_mapped').agg(Leads=('source_mapped', 'size'), MQLs=('qualificacao_pl', lambda x: (x == 'MQL').sum()), Vendas=('real_faturamento_venda_pl', lambda x: (x > 0).sum()), Faturamento=('real_faturamento_venda_pl', 'sum')).reset_index()
    source_agg['perc_mql'] = (source_agg['MQLs'] / source_agg['Leads'] * 100).fillna(0)
    source_counts = df_source_chart['source_mapped'].value_counts(normalize=True).reset_index()
    source_counts.columns = ['source_mapped', 'percentage']
    source_counts['percentage'] *= 100
    source_counts = pd.merge(source_counts, source_agg, on='source_mapped', how='left')
    source_counts['Faturamento_fmt'] = source_counts['Faturamento'].apply(lambda x: format_brazilian(x, "R$ {:,.2f}"))
    source_counts['perc_mql_fmt'] = source_counts['perc_mql'].apply(lambda x: f"{format_brazilian(x, '{:,.1f}')}%")
    source_counts['dummy_y'] = 'Fontes de Tráfego'
    source_counts.sort_values('percentage', ascending=False, inplace=True)
    fig_source = px.bar(source_counts, x='percentage', y='dummy_y', color='source_mapped', orientation='h', height=320, text=source_counts.apply(lambda row: f"{row['percentage']:.1f}%" if row['percentage'] > 2 else '', axis=1), color_discrete_map=color_map, hover_name='source_mapped', custom_data=['Leads', 'MQLs', 'Vendas', 'Faturamento_fmt', 'perc_mql_fmt'])
    fig_source.update_layout(title_text='', xaxis_title="", yaxis_title="", legend_title_text='', plot_bgcolor='rgba(0,0,0,0)', paper_bgcolor='rgba(0,0,0,0)', font_color='white', showlegend=True, xaxis=dict(showgrid=False, zeroline=False, showticklabels=True, dtick=5, ticksuffix='%', range=[0, 100], tickmode='linear'), yaxis=dict(showgrid=False, zeroline=False, showticklabels=False), legend=dict(orientation="h", yanchor="bottom", y=-0.7, xanchor="center", x=0.5), margin=dict(b=180), hoverlabel=dict(font=dict(size=20)))
    fig_source.update_traces(textposition='inside', textfont_size=12, textfont_color='white', hovertemplate=('<b>%{hovertext}</b><br><br>Leads: %{customdata[0]}<br>MQLs: %{customdata[1]}<br>% MQL: %{customdata[4]}<br>Vendas: %{customdata[2]}<br>Faturamento: %{customdata[3]}<extra></extra>'))
    st.plotly_chart(fig_source, use_container_width=True)
else:
    st.info("Dados de `utm_source` não disponíveis para gerar o gráfico de fontes.")

st.markdown(HR_SEPARATOR_STYLE, unsafe_allow_html=True)
# ... O resto do código (seções Google/Meta/Conferidor) continua o mesmo
st.markdown("<h2 style='text-align: center;'>Google ADS</h2>", unsafe_allow_html=True)
if not tabela_gads.empty:
    kpis_gads_data = {}
    investido_gads_total, leads_gads_total, mql_gads_total, total_cliques_gads, total_impressoes_gads = tabela_gads['Investido'].sum(), tabela_gads['Leads'].sum(), tabela_gads['MQL'].sum(), tabela_gads['Cliques'].sum(), tabela_gads['Impressões'].sum()
    kpis_gads_data['Total investido'] = investido_gads_total
    kpis_gads_data['Total de leads'] = leads_gads_total
    kpis_gads_data['Total de MQLs'] = mql_gads_total
    kpis_gads_data['Total de vendas'] = tabela_gads['Vendas'].sum()
    kpis_gads_data['Faturamento'] = tabela_gads['Valor'].sum()
    kpis_gads_data['CTR médio'] = (total_cliques_gads / total_impressoes_gads * 100) if total_impressoes_gads > 0 else 0
    kpis_gads_data['Taxa média de conversão'] = (leads_gads_total / total_cliques_gads * 100) if total_cliques_gads > 0 else 0
    kpis_gads_data['% MQL'] = (mql_gads_total / leads_gads_total * 100) if leads_gads_total > 0 else 0
    kpis_formats = {'Total investido': "R$ {:,.2f}", 'Total de leads': "{:,.0f}", 'Total de MQLs': "{:,.0f}", 'Total de vendas': "{:,.0f}", 'Faturamento': "R$ {:,.2f}", 'CTR médio': "{:.2f}%", 'Taxa média de conversão': "{:.2f}%", '% MQL': "{:.2f}%"}
    kpi_cols = st.columns(4)
    kpi_items = list(kpis_gads_data.items())
    for i in range(len(kpi_items)):
        with kpi_cols[i % 4]:
            name, value = kpi_items[i]
            formatted_value = format_brazilian(value, kpis_formats[name])
            st.metric(label=name, value=formatted_value)
else:
    st.info("Dados do Google Ads não disponíveis para exibir os KPIs.")

tab_gads, tab_kw = st.tabs(["Google Ads", "Palavras-chave"])
with tab_gads:
    if not tabela_gads.empty:
        tabela_gads_display = tabela_gads.copy()
        format_cols_currency, format_cols_percent = ['Investido', 'CPL', 'CPMQL', 'Valor'], ['CTR', 'Taxa de conversão', '% MQL']
        for col in format_cols_currency: tabela_gads_display[col] = tabela_gads_display[col].apply(lambda x: format_brazilian(x, "R$ {:,.2f}"))
        for col in format_cols_percent: tabela_gads_display[col] = tabela_gads_display[col].apply(lambda x: format_brazilian(x, "{:.2f}%"))
        final_cols_gads = ['Campanha', 'Impressões', 'CTR', 'Cliques', 'Taxa de conversão', 'Leads', '% MQL', 'MQL', 'Investido', 'CPL', 'CPMQL', 'Vendas', 'Valor']
        st.dataframe(tabela_gads_display[final_cols_gads], use_container_width=True, hide_index=True)
    else:
        st.info("Dados do Google Ads não disponíveis para o período.")
with tab_kw:
    tabela_kw = pd.DataFrame()
    try:
        if not df_google_kw_bq.empty:
            df_kw_agg = df_google_kw_bq.groupby('keyword_text_gkw').agg(Investido=('cost_gkw', 'sum'), Impressões=('impressions_gkw', 'sum'), Cliques=('clicks_gkw', 'sum')).reset_index()
            if not df_pl_processed_kw.empty:
                df_kw_agg['join_key'] = df_kw_agg['keyword_text_gkw'].str.lower()
                df_pl_processed_kw['join_key'] = df_pl_processed_kw['utm_term_pl'].str.lower()
                tabela_kw = pd.merge(df_kw_agg, df_pl_processed_kw, on='join_key', how='left')
                tabela_kw.drop(columns=['join_key'], inplace=True, errors='ignore')
            else:
                tabela_kw = df_kw_agg.copy()
            numeric_cols_to_fill_kw = ['Leads', 'MQL', 'Vendas', 'Valor']
            for col in numeric_cols_to_fill_kw:
                if col not in tabela_kw.columns: tabela_kw[col] = 0
                tabela_kw[col] = pd.to_numeric(tabela_kw[col], errors='coerce').fillna(0)
            tabela_kw['CTR'] = (tabela_kw['Cliques'] / tabela_kw['Impressões'] * 100).fillna(0)
            tabela_kw['Taxa de conversão'] = (tabela_kw['Leads'] / tabela_kw['Cliques'] * 100).fillna(0)
            tabela_kw['% MQL'] = (tabela_kw['MQL'] / tabela_kw['Leads'] * 100).fillna(0)
            tabela_kw['CPL'] = (tabela_kw['Investido'] / tabela_kw['Leads']).fillna(0)
            tabela_kw['CPMQL'] = (tabela_kw['Investido'] / tabela_kw['MQL']).fillna(0)
            tabela_kw.replace([np.inf, -np.inf], 0.0, inplace=True)
            tabela_kw.rename(columns={'keyword_text_gkw': 'Palavra-chave'}, inplace=True)
            tabela_kw_display = tabela_kw.copy()
            format_cols_currency, format_cols_percent = ['Investido', 'CPL', 'CPMQL', 'Valor'], ['CTR', 'Taxa de conversão', '% MQL']
            for col in format_cols_currency: tabela_kw_display[col] = tabela_kw_display[col].apply(lambda x: format_brazilian(x, "R$ {:,.2f}"))
            for col in format_cols_percent: tabela_kw_display[col] = tabela_kw_display[col].apply(lambda x: format_brazilian(x, "{:.2f}%"))
            final_cols_kw = ['Palavra-chave', 'Impressões', 'CTR', 'Cliques', 'Taxa de conversão', 'Leads', '% MQL', 'MQL', 'Investido', 'CPL', 'CPMQL', 'Vendas', 'Valor']
            st.dataframe(tabela_kw_display[final_cols_kw], use_container_width=True, hide_index=True)
        else:
            st.info("Dados de Palavras-chave do Google não disponíveis para o período.")
    except Exception as e:
        st.warning(f"Não foi possível processar os dados da tabela de Palavras-Chave. Erro: {e}")

st.markdown(HR_SEPARATOR_STYLE, unsafe_allow_html=True)

st.markdown("<h2 style='text-align: center;'>Meta ADS</h2>", unsafe_allow_html=True)
if not df_fb_creatives_final.empty:
    df_meta_ads_agg_kpi = df_fb_creatives_final.groupby('Campanha').agg(Impressões=('Impressões', 'sum'), Cliques=('Cliques', 'sum'), Leads=('Leads', 'sum'), MQL=('MQL', 'sum'), Investido=('Investido', 'sum'), Vendas=('Vendas', 'sum'), Valor=('Valor', 'sum')).reset_index()
    kpis_fb_data = {}
    investido_fb_total, leads_fb_total, mql_fb_total, total_cliques_fb, total_impressoes_fb = df_meta_ads_agg_kpi['Investido'].sum(), df_meta_ads_agg_kpi['Leads'].sum(), df_meta_ads_agg_kpi['MQL'].sum(), df_meta_ads_agg_kpi['Cliques'].sum(), df_meta_ads_agg_kpi['Impressões'].sum()
    kpis_fb_data['Total investido'], kpis_fb_data['Total de leads'], kpis_fb_data['Total de MQLs'] = investido_fb_total, leads_fb_total, mql_fb_total
    kpis_fb_data['Total de vendas'], kpis_fb_data['Faturamento'] = df_meta_ads_agg_kpi['Vendas'].sum(), df_meta_ads_agg_kpi['Valor'].sum()
    kpis_fb_data['CTR médio'] = (total_cliques_fb / total_impressoes_fb * 100) if total_impressoes_fb > 0 else 0
    kpis_fb_data['Taxa média de conversão'] = (leads_fb_total / total_cliques_fb * 100) if total_cliques_fb > 0 else 0
    kpis_fb_data['% MQL'] = (mql_fb_total / leads_fb_total * 100) if leads_fb_total > 0 else 0
    kpis_formats = {'Total investido': "R$ {:,.2f}", 'Total de leads': "{:,.0f}", 'Total de MQLs': "{:,.0f}", 'Total de vendas': "{:,.0f}", 'Faturamento': "R$ {:,.2f}", 'CTR médio': "{:.2f}%", 'Taxa média de conversão': "{:.2f}%", '% MQL': "{:.2f}%"}
    kpi_cols_fb = st.columns(4)
    kpi_items_fb = list(kpis_fb_data.items())
    for i in range(len(kpi_items_fb)):
        with kpi_cols_fb[i % 4]:
            name, value = kpi_items_fb[i]
            st.metric(label=name, value=format_brazilian(value, kpis_formats[name]))
else:
    st.info("Dados de Meta Ads não disponíveis para exibir os KPIs.")
tab_meta, tab_crtv = st.tabs(["Meta Ads", "Criativos"])
with tab_meta:
    if not df_fb_creatives_final.empty:
        df_meta_ads = df_fb_creatives_final.groupby('Campanha').agg(Impressões=('Impressões', 'sum'), Cliques=('Cliques', 'sum'), Leads=('Leads', 'sum'), MQL=('MQL', 'sum'), Investido=('Investido', 'sum'), Vendas=('Vendas', 'sum'), Valor=('Valor', 'sum')).reset_index()
        df_meta_ads['CTR'] = (df_meta_ads['Cliques'] / df_meta_ads['Impressões'] * 100).fillna(0)
        df_meta_ads['Taxa de conversão'] = (df_meta_ads['Leads'] / df_meta_ads['Cliques'] * 100).fillna(0)
        df_meta_ads['% MQL'] = (df_meta_ads['MQL'] / df_meta_ads['Leads'] * 100).fillna(0)
        df_meta_ads['CPL'] = (df_meta_ads['Investido'] / df_meta_ads['Leads']).fillna(0)
        df_meta_ads['CPMQL'] = (df_meta_ads['Investido'] / df_meta_ads['MQL']).fillna(0)
        df_meta_ads.replace([np.inf, -np.inf], 0.0, inplace=True)
        df_meta_ads_display = df_meta_ads.copy()
        format_cols_currency, format_cols_percent = ['Investido', 'CPL', 'CPMQL', 'Valor'], ['CTR', 'Taxa de conversão', '% MQL']
        for col in format_cols_currency: df_meta_ads_display[col] = df_meta_ads_display[col].apply(lambda x: format_brazilian(x, "R$ {:,.2f}"))
        for col in format_cols_percent: df_meta_ads_display[col] = df_meta_ads_display[col].apply(lambda x: format_brazilian(x, "{:.2f}%"))
        final_cols_meta = ['Campanha', 'Impressões', 'CTR', 'Cliques', 'Taxa de conversão', 'Leads', '% MQL', 'MQL', 'Investido', 'CPL', 'CPMQL', 'Vendas', 'Valor']
        st.dataframe(df_meta_ads_display[final_cols_meta], use_container_width=True, hide_index=True)
    else:
        st.info("Não foi possível processar os dados para a tabela de Meta Ads.")
with tab_crtv:
    if not df_fb_creatives_final.empty:
        df_fb_creatives_display = df_fb_creatives_final.copy()
        format_cols_currency, format_cols_percent = ['Investido', 'CPL', 'CPMQL', 'Valor'], ['CTR', 'Taxa de conversão', '% MQL']
        for col in format_cols_currency: df_fb_creatives_display[col] = df_fb_creatives_display[col].apply(lambda x: format_brazilian(x, "R$ {:,.2f}"))
        for col in format_cols_percent: df_fb_creatives_display[col] = df_fb_creatives_display[col].apply(lambda x: format_brazilian(x, "{:.2f}%"))
        final_cols_crtv = ['Campanha', 'Público', 'Criativo', 'Impressões', 'CTR', 'Cliques', 'Taxa de conversão', 'Leads', '% MQL', 'MQL', 'Investido', 'CPL', 'CPMQL', 'Vendas', 'Valor']
        st.dataframe(df_fb_creatives_display[final_cols_crtv], use_container_width=True, hide_index=True)
    else:
        st.info("Dados do Facebook Ads não estão prontos ou completos para gerar a tabela de criativos.")

st.markdown(HR_SEPARATOR_STYLE, unsafe_allow_html=True)

if conferidor_mode:
    st.markdown("<h2 style='text-align: center;'>Modo Conferidor</h2>", unsafe_allow_html=True)
    st.markdown(f"<h3>Período: {data_selecionada_inicio.strftime('%d/%m/%Y')} até {data_selecionada_fim.strftime('%d/%m/%Y')}</h3>", unsafe_allow_html=True)
    st.markdown(HR_SEPARATOR_STYLE, unsafe_allow_html=True)
    st.markdown("<h2>Resumo das Métricas</h2>", unsafe_allow_html=True)
    def formatar_numero_br_conferidor(valor, metrica_config, tipo_valor='realizado'):
        if pd.isna(valor) or valor == float('inf') or valor == float('-inf'): return "N/A"
        formato_str = metrica_config.get("formato", "{:,.2f}")
        if metrica_config.get('nome') == "Taxa de MQL":
             valor_a_formatar = valor * 100 if tipo_valor == 'meta' else valor
             formato_str = "{:.2f}%"
        else: valor_a_formatar = valor
        return format_brazilian(valor_a_formatar, formato_str)
    def formatar_atingido_br(valor):
        if pd.isna(valor) or valor == float('inf') or valor == float('-inf'): return "N/A"
        return format_brazilian(valor, "{:,.2f}%")
    cols_checkin = st.columns(5)
    col_idx_checkin = 0
    for metrica_nome in metricas_para_exibir_checkin:
        if metrica_nome in df_tabela_performance.columns:
            realizado, meta, diferenca, atingido_perc, status = df_tabela_performance.loc[:, metrica_nome]
            config_metrica = metricas_config.get(metrica_nome, {'nome': metrica_nome})
            realizado_fmt, meta_fmt, diferenca_fmt, atingido_fmt = formatar_numero_br_conferidor(realizado, config_metrica, 'realizado'), formatar_numero_br_conferidor(meta, config_metrica, 'meta'), formatar_numero_br_conferidor(diferenca, config_metrica, 'diferenca'), formatar_atingido_br(atingido_perc)
            with cols_checkin[col_idx_checkin % 5]:
                st.markdown(f"""
                <div class="metric-block">
                    <p><strong>{metrica_nome}</strong></p>
                    <p>Realizado: {realizado_fmt}</p>
                    <p>Meta: {meta_fmt}</p>
                    <p>Diferença: {diferenca_fmt}</p>
                    <p>% Atingido: {atingido_fmt}</p>
                    <p>Status: {status}</p>
                </div>""", unsafe_allow_html=True)
            col_idx_checkin += 1
    st.markdown("<h2 style='text-align: center;'>Tabelas de Dados Fontes (Filtradas por Data)</h2>", unsafe_allow_html=True)
    def display_conferidor_table(df_source, title, project_id_col_name):
        st.subheader(f"Tabela Fonte: {title}")
        if df_source is None or df_source.empty:
            st.write(f"Não há dados para exibir para {title} no período selecionado ou a fonte não foi carregada.")
            st.markdown("---")
            return
        df_display = df_source.copy()
        if project_id_col_name and project_id_col_name in df_display.columns:
            df_display = df_display[df_display[project_id_col_name] == selected_project_id]
            cols = [project_id_col_name] + [col for col in df_display.columns if col != project_id_col_name]
            df_display = df_display[cols]
        st.dataframe(df_display, use_container_width=True, hide_index=True)
        st.markdown("---")
    display_conferidor_table(df_pl1_conferidor, "Leads (BQ)", 'project_id_pl')
    display_conferidor_table(df_p2_conferidor, "Planilha 2 (Metas)", 'project_id_p2')
    display_conferidor_table(df_google_kw_bq, "Google Keywords (BQ)", 'project_id_gkw')
    display_conferidor_table(df_gads_bq, "Google Ads (BQ)", 'project_id_gads')
    display_conferidor_table(df_fb_ads_bq, "Facebook Ads (BQ)", 'project_id_fb')
    st.markdown("---")
