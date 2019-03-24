#%%
import pandas as pd
import numpy as np
import itertools
import math
from multiprocessing import Manager, Pool, cpu_count
from functools import partial


#%% converte hora em minutos
def horario_em_mins(horario):
    h = str(horario.strftime("%H:%M:%S")).split(':')
    m = int(h[0]) * 60 + int(h[1])
    return m    


# verifica se ha choque de horario entre eventos
def ha_choque(a, b):
    return max(0, min(horario_em_mins(a[1]), horario_em_mins(b[1])) - 
                  max(horario_em_mins(a[0]), horario_em_mins(b[0])))


# padroniza valor numa faixa de 0 a 1
def max_min(valor, minimo, maximo):
    novo_valor = None

    if valor is not None:
        novo_valor = (valor - minimo) / (maximo - minimo)
        
    return novo_valor


# monta a agenda recebendo horario de inicio e fim 
def monta_agenda(agenda, df):
    print(agenda)
    # eventos por horario
    ev = df.loc[(df['dia'] == agenda['dia']) & \
                (df['inicio'] >= pd.to_datetime(agenda['inicio']).time()) & \
                (df['inicio'] < pd.to_datetime(agenda['fim']).time())]

    # criar combinacoes eventos manha
    combos = list(itertools.combinations(ev['id'].tolist(), 2))

    # retirar combinacoes que chocam
    ls = []
    for combo in combos:
        # recupera dados dos eventos
        ev_1 = ev.loc[ev['id'] == combo[0]].to_dict('r')[0]
        ev_2 = ev.loc[ev['id'] == combo[1]].to_dict('r')[0]

        if not ha_choque((ev_1['inicio'], 
                          ev_1['fim']), 
                         (ev_2['inicio'], 
                          ev_2['fim'])):
            d = {'dia': agenda['dia'], 'inicio': agenda['inicio'], 'evento_1' : combo[0], 'evento_2': combo[1]}

            # calcular custos das combinacoes
            d['prioridade'] = ev_1['prioridade'] + ev_2['prioridade']
            d['distancia'] = math.sqrt( (ev_1['latitude'] - ev_2['latitude']) ** 2 + (ev_1['longitude'] - ev_2['longitude']) ** 2 )
            d['acesso'] = ev_1['acesso'] + ev_2['acesso']
            ls.append(d)    
    return ls


#%% constantes
ENTRADA = 'prioridade_mourao.csv'
SAIDA = 'selecao_mourao.csv'
SAIDA2 = 'selecao_alternativa_mourao.csv'

#%% recupera eventos com suas prioridades
df = pd.read_csv('dados/' + ENTRADA, sep='|')

#%% ajusta dados
# converte campos de hora para formato datetime
df['inicio'] = pd.to_datetime(df['inicio']).dt.time
df['fim'] = pd.to_datetime(df['fim']).dt.time
# remove registros com horarios nulos
df = df.loc[~(df['inicio'].isnull() | df['fim'].isnull())]
# substitui meia-noite para um segundo antes, de forma que nao haja confusao com
# os calculos
df.loc[df['fim'] == pd.to_datetime('00:00:00').time(), 'fim'] = pd.to_datetime('23:59:59').time()
# remove eventos de mentoria
df = df.loc[df['mentoria'] == 0]
# remove eventos sem acesso
df = df.loc[df['acesso'] < 1]
# remove eventos muito curtos e muito longos
df['duracao'] = df.apply(lambda x: horario_em_mins(x['fim']) - horario_em_mins(x['inicio']), axis=1)
#df = df.loc[(df['duracao'] >= 30) & (df['duracao'] <= 180)]

#%% recupera dias e turnos do evento
dias = [x for x in list(set(df['dia'])) if str(x) != 'nan']
turnos = [['06:00:00', '13:00:00'], ['14:00:00', '19:00:00']]
agenda = []
for dia in dias:
    for turno in turnos:
        agenda.append({'dia': dia, 'inicio': turno[0], 'fim': turno[1]})
print(agenda)

#%% executa a montagem das possibilidades, dividindo o trabalho em processos
# deixa duas cpus livres para o sistema operacional
cpus = cpu_count() - 2
# monta opcoes
with Pool(processes=cpus,) as pool:
    ls =  pool.map(partial(monta_agenda, df=df), agenda)
flat_list = [item for sublist in ls for item in sublist]
df_combos = pd.DataFrame(flat_list)


#%% normaliza colunas de custo
minimo, maximo = (df_combos['prioridade'].min(), df_combos['prioridade'].max())
df_combos['prioridade'] = df_combos.apply(lambda x : \
                                          max_min(x['prioridade'], minimo, maximo), 
                                          axis=1)
minimo, maximo = (df_combos['acesso'].min(), df_combos['acesso'].max())
df_combos['acesso'] = df_combos.apply(lambda x : \
                                      max_min(x['acesso'], minimo, maximo), 
                                      axis=1)
minimo, maximo = (df_combos['distancia'].min(), df_combos['distancia'].max())
df_combos['distancia'] = df_combos.apply(lambda x : \
                                         max_min(x['distancia'], minimo, maximo), 
axis=1)


#%% calcula custo total
df_combos['custo'] = df_combos['prioridade'] + \
                     df_combos['acesso']  + \
                     df_combos['distancia']  
df_combos.sort_values(by=['dia', 'inicio', 'custo'], inplace=True)
df_combos.to_csv('dados/' + SAIDA, sep='|', index=False)

#%% primeira agenda
selecao = df_combos.groupby(by=['dia', 'inicio']).first()

# salva agenda
df_selecao = df.loc[df['id'].isin(selecao['evento_1'].tolist() + selecao['evento_2'].tolist())]
df_selecao.to_csv('dados/' + SAIDA, sep='|', index=False)
print(df_selecao)

#%% segunda agenda
fora = list(selecao['evento_1']) + list(selecao['evento_2'])
selecao2 = df_combos[(~df_combos['evento_1'].isin(fora)) & (~df_combos['evento_2'].isin(fora))].groupby(by=['dia', 'inicio']).first()

# salva segunda agenda
df_selecao2 = df.loc[df['id'].isin(selecao2['evento_1'].tolist() + selecao2['evento_2'].tolist())]
df_selecao2.to_csv('dados/' + SAIDA2, sep='|', index=False)
print(df_selecao2)