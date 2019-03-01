#%%
import pandas as pd
import numpy as np
import itertools
import math


#%%
def horario_em_mins(horario):
    h = str(horario.values[0].strftime("%H:%M:%S")).split(':')
    m = int(h[0]) * 60 + int(h[1])
    return m


def ha_choque(a, b):
    return max(0, min(horario_em_mins(a[1]), horario_em_mins(b[1])) - 
                  max(horario_em_mins(a[0]), horario_em_mins(b[0])))


def max_min(valor, minimo, maximo):
    novo_valor = None

    if valor is not None:
        novo_valor = (valor - minimo) / (maximo - minimo)
        
    return novo_valor


#%%
ENTRADA = 'prioridade_mourao.csv'
SAIDA = 'selecao_mourao.csv'

#%%
df = pd.read_csv('dados/' + ENTRADA, sep='|')

#%%
df['inicio'] = pd.to_datetime(df['inicio']).dt.time
df['fim'] = pd.to_datetime(df['fim']).dt.time

#%%
dias = [x for x in list(set(df['dia'])) if str(x) != 'nan']
print(dias)

#%%
ls_tudo = []
for dia in dias:
    print(dia)

    # eventos da manha
    ev = df.loc[(df['dia'] == dia) & \
                (df['inicio'] >= pd.to_datetime('06:00:00').time()) & \
                (df['inicio'] < pd.to_datetime('12:00:00').time())]

    # criar combinacoes eventos manha
    combos = list(itertools.combinations(ev['id'].tolist(), 2))

    # retirar combinacoes que chocam
    for combo in combos:
        # recupera dados dos eventos
        ev_1 = ev.loc[ev['id'] == combo[0]]
        ev_2 = ev.loc[ev['id'] == combo[1]]

        if not ha_choque((ev_1['inicio'], ev_1['fim']), (ev_2['inicio'], ev_2['fim'])):
            d = {'dia': dia, 'turno': 'manha', 'evento_1' : combo[0], 'evento_2': combo[1]}

            # calcular custos das combinacoes
            d['prioridade'] = ev_1['prioridade'].values[0] + ev_2['prioridade'].values[0]
            d['mentoria'] = ev_1['mentoria'].values[0] + ev_2['mentoria'].values[0]
            d['distancia'] = math.sqrt( (ev_1['latitude'].values[0]-ev_2['latitude'].values[0])**2 + 
                                   (ev_1['longitude'].values[0]-ev_2['longitude'].values[0])**2 )
            d['acesso'] = ev_1['acesso'].values[0] + ev_2['acesso'].values[0]
            ls_tudo.append(d)

    # eventos da tarde
    ev = df.loc[(df['dia'] == dia) & \
                (df['inicio'] >= pd.to_datetime('14:00:00').time()) & \
                (df['inicio'] < pd.to_datetime('18:30:00').time())]

    # criar combinacoes eventos tarde
    combos = list(itertools.combinations(ev['id'].tolist(), 2))

    # retirar combinacoes que chocam
    for combo in combos:
        # recupera dados dos eventos
        ev_1 = ev.loc[ev['id'] == combo[0]]
        ev_2 = ev.loc[ev['id'] == combo[1]]

        if not ha_choque((ev_1['inicio'], ev_1['fim']), (ev_2['inicio'], ev_2['fim'])):
            d = {'dia': dia, 'turno' : 'tarde', 'evento_1' : combo[0], 'evento_2': combo[1]}

            # calcular custos das combinacoes
            d['prioridade'] = ev_1['prioridade'].values[0] + ev_2['prioridade'].values[0]
            d['mentoria'] = ev_1['mentoria'].values[0] + ev_2['mentoria'].values[0]
            d['distancia'] = math.sqrt( (ev_1['latitude'].values[0]-ev_2['latitude'].values[0])**2 + 
                                   (ev_1['longitude'].values[0]-ev_2['longitude'].values[0])**2 )
            ls_tudo.append(d)                      
df_combos = pd.DataFrame(ls_tudo)

#%%
min_prioridade = df_combos['prioridade'].min()
max_prioridade = df_combos['prioridade'].max()
min_mentoria = df_combos['mentoria'].min()
max_mentoria = df_combos['mentoria'].max()
min_distancia = df_combos['distancia'].min()
max_distancia = df_combos['distancia'].max()
min_acesso = df_combos['acesso'].min()
max_acesso = df_combos['acesso'].max()



#%%
df_combos['custo'] = df_combos.apply(lambda x : max_min(x['prioridade'], min_prioridade, max_prioridade) + 
                                                max_min(x['mentoria'], min_mentoria, max_mentoria) * 2.0 +
                                                max_min(x['distancia'], min_distancia, max_distancia) +
                                                max_min(x['acesso'], min_acesso, max_acesso) * 2.0, axis=1)

#%%
df_combos.sort_values(by=['dia', 'turno', 'custo'], inplace=True)
df_combos.to_csv('dados/' + SAIDA, sep='|', index=False)

#%%
selecao = df_combos.groupby(by=['dia', 'turno']).first()

#%%
df.loc[df['id'].isin(selecao['evento_1'].tolist() + selecao['evento_2'].tolist())].to_csv('dados/' + SAIDA, sep='|', index=False)
