# =============================================================================
# Facebook Ads Insights – Extração e Tratamento
# =============================================================================

import os
import datetime
from io import StringIO
import logging
import boto3
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights

# -----------------------------------------------------------------------------
# 1. CONFIGURAÇÕES INICIAIS
# -----------------------------------------------------------------------------

#Em muitos de meus projetos, utilizo o sistema da AWS BucketS3 para armazenar meus arquivos e credenciais.
# Carrega variáveis de ambiente a partir de um arquivo .env 
BUCKET_NAME       = 'NOME_DO_SEU_BUCKET'           # ex: 'meu-datalake'
CREDENTIALS_KEY   = 'caminho/para/credentials.env' # ex: 'prod/credentials/facebook.env'

s3 = boto3.client('s3')
# Lê o arquivo de credenciais do S3 e carrega no ambiente
env_content = s3.get_object(Bucket=BUCKET_NAME, Key=CREDENTIALS_KEY)['Body'].read().decode()
load_dotenv(stream=StringIO(env_content))

#Aqui você ira carregar todas as credenciais que obteve no aplicativo gerado no meta...
# Obtém credenciais da API a partir das variáveis de ambiente
FB_APP_ID         = os.getenv('FACEBOOK_APP_ID')        # ex: '123456789012345'
FB_APP_SECRET     = os.getenv('FACEBOOK_APP_SECRET')    # ex: 'abcdef1234567890'
FB_ACCESS_TOKEN   = os.getenv('FACEBOOK_ACCESS_TOKEN')  # ex: 'EAA...ZD'

# Pré-configurações de data -- Nesta parte não é uma regra, fica a vontade para modificar :).
TODAY      = datetime.datetime.now().strftime('%Y-%m-%d')
LAST_DAYS  = 30                        # número de dias para captação
BASE_DATE  = '2024-08-22'              # data mínima (YYYY-MM-DD)
LEVEL      = 'campaign'                # nível de agregação ('campaign', 'adset' ou 'ad')
TIME_INC   = 1                         # quebra por dia

# IDs de exemplo de contas de anúncio (substitua pelos seus)
AD_ACCOUNT_IDS = [
    'act_1234567890',
    'act_0987654321',
    # …
]

# -----------------------------------------------------------------------------
# 2. FUNÇÕES AUXILIARES
# -----------------------------------------------------------------------------

def initialize_api(app_id: str, app_secret: str, token: str):
    # Inicializa a conexão com a Facebook Marketing API.
    FacebookAdsApi.init(app_id, app_secret, token)

def fetch_insights(
    account_ids: list,
    days_back: int,
    base_date: str,
    level: str,
    time_increment: int
) -> pd.DataFrame:

    # Consulta múltiplas contas e retorna um DataFrame com os insights solicitados.
 
    initialize_api(FB_APP_ID, FB_APP_SECRET, FB_ACCESS_TOKEN)

    # Define intervalo de datas
    end_date   = datetime.datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.datetime.now() - datetime.timedelta(days=days_back)).strftime('%Y-%m-%d')
    # Garante que não passe da data base
    if start_date < base_date:
        start_date = base_date

    time_range = {'since': start_date, 'until': end_date}

    params = {
        'time_range': time_range,
        'level': level,
        'time_increment': time_increment,
        'fields': [
            AdsInsights.Field.campaign_name,
            AdsInsights.Field.campaign_id,
            AdsInsights.Field.impressions,
            AdsInsights.Field.clicks,
            AdsInsights.Field.spend,
            AdsInsights.Field.actions,
            AdsInsights.Field.action_values,
        ],
    }

    all_dfs = []
    for acct in account_ids:
        try:
            account = AdAccount(acct)
            insights = account.get_insights(params=params)
            df = pd.DataFrame([i for i in insights])
            all_dfs.append(df)
        except Exception as e:
            logging.warning(f'Erro conta {acct}: {e}')

    # Concatena todos os resultados
    return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

#Esta parte é opcional, para meu dia a dia atual, esses dados são necessário.
# O campo actions na resposta da Insights API é uma lista de objetos, onde cada objeto representa um tipo de interação que 
# aconteceu após o anúncio ser exibido.
def clean_actions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Garante que 'actions' seja lista
    df['actions'] = df['actions'].apply(lambda x: x.tolist() if isinstance(x, np.ndarray) else x or [])

    types, targets, dests, link_clicks = [], [], [], []

    for actions in df['actions']:
        if actions:
            types.append(actions[0].get('action_type'))
            targets.append(actions[0].get('action_target_id'))
            dests.append(actions[0].get('action_destination'))
            lc = next((float(a.get('value', 0)) for a in actions if a.get('action_type')=='link_click'), 0)
            link_clicks.append(lc)
        else:
            types.append(None); targets.append(None); dests.append(None); link_clicks.append(0)

    # Adiciona colunas ao DataFrame
    df['action_type']        = types
    df['action_target_id']   = targets
    df['action_destination'] = dests
    df['link_clicks']        = link_clicks

    # Converte datas para formato YYYY-MM-DD e unifica campo
    df['date_start'] = df['date_start'].str.split('T').str[0]
    df['date']       = df['date_start']

    return df

# -----------------------------------------------------------------------------
# 3. EXEMPLO DE USO
# -----------------------------------------------------------------------------

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    # 1) Coleta os dados brutos
    raw_df = fetch_insights(
        account_ids=AD_ACCOUNT_IDS,
        days_back=LAST_DAYS,
        base_date=BASE_DATE,
        level=LEVEL,
        time_increment=TIME_INC
    )

    # 2) Aplica o tratamento nas colunas de ações
    cleaned_df = clean_actions(raw_df)

