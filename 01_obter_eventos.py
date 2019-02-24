#%%
import requests
import urllib
import pandas as pd
from bs4 import BeautifulSoup, NavigableString, Tag
import re
import numpy as np
from multiprocessing.dummy import Pool as ThreadPool
import googlemaps
from datetime import datetime
import time

#%%
# chave de autenticacao do google maps
with open('.secret', 'r') as f:
    API_KEY = f.read().strip('\n')
gmaps = googlemaps.Client(key=API_KEY)

#%%
def recuperar_eventos(url):
    print(url)

    # recupera pagina web
    resposta = requests.get(url)

    # retira caracteres especiais repetidos da pagina, web deixando-a mais legivel
    pagina = BeautifulSoup(resposta.text, 'html5lib')

    # encontra trechos com a descricao dos eventos
    eventos = pagina.find_all('div', class_='single-event')

    # percorre a lista, obtendo detalhes dos eventos
    ls = []
    for evento in eventos:
        ls.append('https://schedule.sxsw.com' + evento['data-event-url'])
    
    return ls

#%%
def recuperar_detalhes(url):
    time.sleep(1)
    print(url)

    # recupera pagina web
    resposta = requests.get(url)

    # retira caracteres especiais repetidos da pagina, web deixando-a mais legivel
    pagina = BeautifulSoup(resposta.text, 'html5lib')

    # recupera nome do evento
    try:
        evento = pagina.find('h1', class_='event-name').text
    except AttributeError:
        evento = None


    # encontra data e horario
    try:
        data_horario = pagina.find('div', class_='event-date').string
        data = re.search('^.*(?=( \|))', data_horario).group(1)        
        inicio, fim = recuperar_horarios(data_horario)            
    except AttributeError:
        data = None
        inicio = None
        fim = None

    # encontra nome do local
    try:
        local = pagina.find('header', class_='venue-title')
        local_links = local.find_all('a')
        nome_local = ' '.join([x.string for x in local_links])
    except AttributeError:
        nome_local = None

    # encontra endereco
    try:
        endereco = pagina.find('span', 'address').text + ' Austin, TX, EUA'
    except AttributeError:
        endereco = None
        latitude = None
        longitude = None

    # encontra resumo
    try:
        corpo = pagina.find('div', class_='body')
        paragrafos = corpo.find_all('p')
        resumo = ' '.join([x.text for x in paragrafos]).replace('\n', ' ')
    except AttributeError:
        resumo = None

    return {'evento': evento,
            'data': data, 
            'inicio': inicio, 
            'fim': fim, 
            'local': nome_local, 
            'endereco': endereco, 
            'resumo': resumo}   

#%%
def recuperar_horarios(txt):
    re1='.*?'	# Non-greedy match on filler
    re2='((?:(?:[0-1][0-9])|(?:[2][0-3])|(?:[0-9])):(?:[0-5][0-9])(?::[0-5][0-9])?(?:\\s?(?:am|AM|pm|PM))?)'	# HourMinuteSec 1
    re3='.*?'	# Non-greedy match on filler
    re4='((?:(?:[0-1][0-9])|(?:[2][0-3])|(?:[0-9])):(?:[0-5][0-9])(?::[0-5][0-9])?(?:\\s?(?:am|AM|pm|PM))?)'	# HourMinuteSec 2
    rg = re.compile(re1+re2+re3+re4,re.IGNORECASE|re.DOTALL)
    m = rg.search(txt)
    try:
        inicio = m.group(1)
    except:
        inicio = None
    
    try:
        fim = m.group(2)
    except:
        fim = None

    return( (inicio, fim) )

#%%
def recuperar_coordenadas(gmaps, endereco):
    geocode_result = gmaps.geocode(endereco)
    coords = geocode_result[0]['geometry']['location']
    return( (coords['lat'], coords['lng']) )

#%%
# inicio
ts_inicio = datetime.now()

#%%
# paginas de eventos por dia
urls_principais = ['https://schedule.sxsw.com/2019/03/' + '{:02d}'.format(x) + '/events' for x in range(8, 18)] 

#%%
# recuperar urls de detalhes dos eventos
pool = ThreadPool(8)
resultado = pool.map(recuperar_eventos, urls_principais)

#%%
# fechar pool e esperar threads finalizarem
pool.close()
pool.join()

#%%
# achata lista de listas em lista
urls_detalhes = [item for sublista in resultado for item in sublista]

#%%
# recuperar detalhes
pool = ThreadPool(8)
resultado = pool.map(recuperar_detalhes, urls_detalhes)

#%%
# criar dataframe pandas
eventos_pd = pd.DataFrame(resultado)

#%%
# recuperar enderecos unicos
enderecos = list(set(eventos_pd['endereco'].tolist()))

#%%
# usar a API do Google Maps para recuperar as coordenadas geograficas
ls = []
for endereco in enderecos:
    try:
        lat, lon = recuperar_coordenadas(gmaps, endereco)
        d = {'endereco': endereco}
        d['latitude'] = lat
        d['longitude'] = lon
        ls.append(d)
        time.sleep(5)
    except:
        pass

#%%
enderecos_pd = pd.DataFrame(ls)

#%%
eventos_pd = eventos_pd.merge(enderecos_pd, on='endereco')

#%%
# salvar informacao
eventos_pd.to_csv('dados/eventos.csv', index=True, index_label='id', sep='|')

#%%
# fim
ts_fim = datetime.now()
delta = ts_fim - ts_inicio
print(delta)
