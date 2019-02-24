#%%
import pandas as pd 
import nltk
from nltk.corpus import stopwords  
import gensim
from gensim import parsing
from gensim.models import TfidfModel
from gensim.corpora import Dictionary
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.model_selection import train_test_split
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import make_pipeline
nltk.download('stopwords')


#%%
# criar preprocessamento inicial
class Pre_Pro_01(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass

    def fit(self, df, y=None, **fit_params):
        "Encontra valores necessarios para transformacoes."

        # dados para normalizacao
        self.target_minimo = df['target'].min()
        self.target_maximo = df['target'].max()
        return self

    def transform(self, df):
        "Aplica transformacoes."

        # normalizar alvo
        df['target'] = df.apply(lambda x : self._max_min(x['target']), axis=1)

        # unificar campos nome do evento e resumo
        df['texto'] = df['evento'] + ' ' +  df['resumo']
        
        # efetuar limpeza do texto
        df['texto'] = df.apply(lambda x : self.limpar_texto(x['texto']), axis=1)

        # devolver somente colunas que serao usadas
        df = df[['id', 'texto', 'target']]
        
        return df

    def _max_min(self, valor):
        novo_valor = None

        if valor:
            novo_valor = (valor - self.target_minimo) / (self.target_maximo - self.target_minimo)
        
        return novo_valor

    def limpar_texto(self, texto):
        # forcar formato
        texto = str(texto)

        # caixa baixa
        texto = texto.lower()

        # tirar espacos duplos
        texto = gensim.corpora.textcorpus.strip_multiple_whitespaces(texto)
        
        # remover pontuacao
        texto = gensim.parsing.preprocessing.strip_punctuation(texto)
        
        # retirar numeros
        texto = gensim.parsing.preprocessing.strip_numeric(texto)
        
        # tirar radicais
        texto = gensim.parsing.preprocessing.stem_text(texto)

        # remove stop words
        stops = set(stopwords.words("english"))
        stops.add('sxsw')
        stops.add('rsvp')
        ls_texto = [palavra for palavra in texto.split() if palavra not in stops]

        return ls_texto


#%%
# transformacao tf-idf
class Pre_Pro_02(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass

    def fit(self, df, y=None, **fit_params):
        from gensim.models import TfidfModel

        lista_textos = df['texto'].tolist()
        self.dic = Dictionary(lista_textos)
        corpus = [self.dic.doc2bow(linha) for linha in lista_textos]
        self.tf_idf = TfidfModel(corpus)

    def transform(self, df):
        df['tfidf'] = df.apply(lambda x : self.tf_idf[self.dic.doc2bow(x['texto'])], axis=1)

        ls = []
        for ix, linha in df.iterrows():
            d = {'id': linha.id, 'target': linha.target}
            for token in linha.tfidf:
                d['t_' + str(token[0])] = token[1]
            ls.append(d)
        df = pd.DataFrame(ls)

        df = df.fillna(0)

        return df


#%%
# recuperar arquivo xlsx
df = pd.read_excel('dados/eventos.xlsx', index_col=None)

#%%
# preparacao da variavel target e do texto
pre_pro_01 = Pre_Pro_01()
pre_pro_01.fit(df=df)
df2 = pre_pro_01.transform(df)

#%%
df2.head()

#%%
# vetorizacao do texto com tf-idf
pre_pro_02 = Pre_Pro_02()
pre_pro_02.fit(df=df2)
df3 = pre_pro_02.transform(df2)
df3.head()

#%%
explicativas = [x for x in df3.columns if x not in ['id', 'target']]

#%%
# separar base para criação do modelo
base = df3.loc[df3.target > 0]
len(base)

#%%
## separar em treino e teste
treino, teste = train_test_split(base, random_state=2019)

#%%
regr = RandomForestRegressor(n_estimators=10, random_state=2019)
regr.fit(treino[explicativas], treino['target'])

#%%
regr.score(treino[explicativas], treino['target'])


#%%
regr.score(teste[explicativas], teste['target'])


#%%
casos_novos = df3.loc[df3.target == 0]
predicoes = regr.predict(casos_novos[explicativas])


#%%
casos_novos['target'] = predicoes
casos_novos[['id', 'target']]


#%%
# saida com predicoes
preditos = base[['id', 'target']].append(casos_novos[['id', 'target']]) 
preditos.columns = ['id', 'prioridade']
preditos.head()

#%%
df4 = df.merge(preditos, on='id')
df4.head()



#%%
df4.to_csv('dados/eventos_prioridade.csv', index=False, sep="|")


