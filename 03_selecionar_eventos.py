#%% importa bibliotecas
import pandas as pd
import numpy as np
import copy
from multiprocessing import Manager, Pool
from functools import partial


#%% converte hora em minutos
def horario_em_mins(horario):
    h = str(horario.strftime("%H:%M:%S")).split(':')
    m = int(h[0]) * 60 + int(h[1])
    return m


#%% padroniza valor numa faixa de 0 a 1
def max_min(valor, minimo, maximo):
    novo_valor = None

    if valor is not None:
        novo_valor = (valor - minimo) / (maximo - minimo)
        
    return novo_valor


#%% monta as combinacoes de agenda recursivamente
def monta_agenda(dia, df, agenda, combos):
    df2 = df.loc[df['dia'] == dia].sort_values(by='inicio')
    atual = df2.head(1).to_dict('r')[0]
    df2 = df2.loc[df2['id'] != atual['id']]

    if len(df2) > 0 and len(agenda) < 2:
        inicio = atual['inicio']
        fim = atual['fim']

        sem_choque = df2.loc[df2['inicio'] > fim ]
        if len(sem_choque) > 0:
            monta_agenda(dia, sem_choque, agenda + [atual], combos)

        # tratar demais
        com_choque = df2.loc[((df2['inicio'] >= inicio) & (df2['inicio'] <= fim )) | \
                            ((df2['fim'] >= inicio) & (df2['fim'] <= fim ))]
        if len(com_choque) > 0:
            monta_agenda(dia, df2, agenda, combos)

    else:
        nova_agenda = agenda + [atual]
        prioridade, acesso, distancia = calcula_custos(nova_agenda)
        combos.append({ 'dia' : dia , 
                        'combos' : nova_agenda,
                        'prioridade' : prioridade,
                        'acesso' : acesso,
                        'distancia' : distancia })
        

#%% calcula custos, retornando soma das prioridades, 
#   das distancias e dos acessos
def calcula_custos(agenda):
    from math import sqrt

    df = pd.DataFrame(agenda)
    ordenado = df.sort_values(by='inicio')
    prioridade = 0
    acesso = 0
    distancia = 0
    u_ix = None
    for ix, linha in ordenado.iterrows():
        prioridade += linha['prioridade']
        acesso += linha['acesso']

        if u_ix:
            distancia = sqrt((linha['latitude'] - 
                              ordenado.loc[u_ix, 'latitude'])**2 + 
                             (linha['longitude'] - 
                             ordenado.loc[u_ix, 'longitude'])**2)
            distancia += distancia
        u_ix = ix
    return (prioridade, acesso, distancia)


#%% constantes
ENTRADA = 'prioridade_mourao.csv'
SAIDA = 'selecao_mourao.csv'

#%% recupera eventos com suas prioridades
df = pd.read_csv('dados/' + ENTRADA, sep='|')

#%% ajusta dados

# converte campos de hora para formato datetime
df['inicio'] = pd.to_datetime(df['inicio']).dt.time
df['fim'] = pd.to_datetime(df['fim']).dt.time

# remove registros com horarios nulos
df = df.loc[~(df['inicio'].isnull() | df['fim'].isnull())]

# substitui meia-noite para um minuto antes, de forma que nao haja confusao com
# os calculos
df.loc[df['fim'] == pd.to_datetime('00:00:00').time(), 'fim'] = pd.to_datetime('23:59:59').time()

# remove eventos de mentoria
df = df.loc[df['mentoria'] == 0]

# remove eventos sem acesso
df = df.loc[df['acesso'] < 1]

# remove eventos muito curtos e muito longos
df['duracao'] = df.apply(lambda x: horario_em_mins(x['fim']) - horario_em_mins(x['inicio']), axis=1)
df = df.loc[(df['duracao'] >= 30) & (df['duracao'] <= 180)]

#%% recupera dias do evento
dias = [x for x in list(set(df['dia'])) if str(x) != 'nan']
print(dias)

#%% montar combinacoes possiveis

# # cria lista que pode ser vista por todos os processos
# manager = Manager()
# combos = manager.list()
# # executa a montagem das possibilidades, dividindo o trabalho
# # em processos
# with Pool(processes=3,) as pool:
#     pool.map(partial(monta_agenda, df=df, agenda=[], combos=combos), dias)

# salva as combinacoes em um dataframe pandas    
combos = []
monta_agenda(dia=8, df=df, agenda=[], combos=combos)
df_combos = pd.DataFrame(list(combos))

#%% 
df_combos.head()
len(df_combos)

#%%
df_combos.to_csv('dados/combos.csv', index=False)

