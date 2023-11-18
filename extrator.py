#%%%
import requests
import json
import pandas as pd
from models import *
from time import sleep
# %%
# %%
# %%
sql = 'select "PRONAC" from versalic_projeto'
with get_connection().connect() as conn:
    query = conn.execute(text(sql))         
    ja_tem_id = [str(x) for x in pd.DataFrame(query.fetchall())['PRONAC'].to_list()]
ja_tem_id

#%%
for o in range(0,30000,100):
    print(o)
    url = f'http://api.salic.cultura.gov.br/v1/projetos/?limit=100&offset={o}&sort=PRONAC:desc&format=json'
    req = requests.get(url)
    dados = req.json()['_embedded']['projetos']
    for i,y in enumerate(dados):
        dados[i][f'links_proponente'] = dados[i]['_links']['proponente'].split('/')[-1]
        del dados[i]['_links']
    df = pd.DataFrame(dados)
    df = df[~df['PRONAC'].isin(ja_tem_id)]
    ja_tem_id += df['PRONAC'].to_list()
    print(df.shape)
    if df.shape[0]>0:
        df.to_sql('versalic_projeto', con= get_connection(), if_exists='append', index=False)
    sleep(5)
# %%

sql = "select \"PRONAC\" from versalic_projeto where ((area = 'Artes Cênicas') AND (segmento = 'Apresentação ou Performance de Dança'))"
with get_connection().connect() as conn:
    query = conn.execute(text(sql))         
    id_danca = [str(x) for x in pd.DataFrame(query.fetchall())['PRONAC'].to_list()]

for PRONAC in id_danca:
    url2 = f'http://api.salic.cultura.gov.br/v1/projetos/{PRONAC}'
    dados2 = requests.get(url2)
    df2 = pd.DataFrame(dados2.json()['_embedded']['relatorio_fisco'])
    df2['PRONAC'] = PRONAC
    df2['valor_unit'] = df2['valor_programado'] / df2['qtd_programada']
    df2.to_sql('versalic_fisco', con= get_connection(), if_exists='append', index=False)
    sleep(3)
# %%

# %%
