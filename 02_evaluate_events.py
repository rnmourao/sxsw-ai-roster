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
# standardizes the event ranking variable (target). Simplifies the text found in the events' title and abstract.
class Pre_Pro_01(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass

    # finds the values used to standardization.
    def fit(self, df, y=None, **fit_params):
        self.target_min = df['target'].min()
        self.target_max = df['target'].max()
        return self

    # applies transformations.
    def transform(self, df):
        df['target'] = df.apply(lambda x : self._max_min(x['target']), axis=1)
        df['text'] = df['event'] + ' ' +  df['abstract']
        df['text'] = df.apply(lambda x : self.clean_text(x['text']), axis=1)
        df = df[['id', 'text', 'target']]
        return df

    # standardization function
    def _max_min(self, value):
        new_value = None

        if value is not None:
            new_value = (value - self.target_min) / (self.target_max - self.target_min)
        
        return new_value

    # function to clean a text
    def clean_text(self, text):
        text = str(text)
        text = text.lower()
        text = gensim.corpora.textcorpus.strip_multiple_whitespaces(text)
        text = gensim.parsing.preprocessing.strip_punctuation(text)
        text = gensim.parsing.preprocessing.strip_numeric(text)
        text = gensim.parsing.preprocessing.stem_text(text)
        stops = set(stopwords.words("english"))
        stops.add('sxsw')
        stops.add('rsvp')
        ls_text = [word for word in text.split() if word not in stops]
        return ls_text


#%%
# transforms text in a word's frequency table, using tf-idf algorithm
class Pre_Pro_02(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass

    # generates a frequency table based on all the words found in the events
    def fit(self, df, y=None, **fit_params):
        from gensim.models import TfidfModel

        ls_texts = df['text'].tolist()
        self.dic = Dictionary(ls_texts)
        corpus = [self.dic.doc2bow(row) for row in ls_texts]
        self.tf_idf = TfidfModel(corpus)


    # converts the text of each event into its frequencies
    def transform(self, df):
        df['tfidf'] = df.apply(lambda x : self.tf_idf[self.dic.doc2bow(x['text'])], axis=1)

        ls = []
        for ix, row in df.iterrows():
            d = {'id': row.id, 'target': row.target}
            for token in row.tfidf:
                d['t_' + str(token[0])] = token[1]
            ls.append(d)
        df = pd.DataFrame(ls)

        features = [x for x in df.columns if x not in ['id', 'target']]
        df[features] = df[features].fillna(0)

        return df


#%%
# filenames
IN = 'events_mourao.xlsx'
OUT = 'rank_mourao.csv'

#%%
# gets ranked events
df = pd.read_excel('data/' + IN, index_col=None)

#%%
# does the data preparation
text_prep = Pipeline([
                      ('clean', Pre_Pro_01),
                      ('freq', Pre_Pro_02)
                     ])
freqs = text_prep.fit_transform(df)

#%%
# selects data to use in the learning step
data = freqs.loc[freqs['target'].notnull()]
len(data)

#%%
## splits data in train and test sets
train, test = train_test_split(data, random_state=2019, test_size=.3)

#%%
# explanatory variables
features = [x for x in freqs.columns if x not in  ['id', 'target']]

#%%
# trains the model using Random Forest algorithm
regr = RandomForestRegressor(n_estimators=20, random_state=2019)
regr.fit(train[features], train['target'])

#%%
# shows the training score
regr.score(train[features], train['target'])


#%%
# shows the test score
regr.score(test[features], test['target'])


#%%
# uses the model to rank the events
new_cases =  freqs.loc[ freqs['target'].isnull()]
predictions = regr.predict(new_cases[features])


#%%
# creates a new dataframe joining the user's ranked events with the predicted ones
new_cases['target'] = predictions
predicted = base[['id', 'target']].append(new_cases[['id', 'target']]) 
predicted.columns = ['id', 'rank']
predicted.head()

#%%
# merges the predictions with the original dataframe
df = df.merge(predicted, on='id')
df.head()

#%%
# selects the useful attributes
df = df[['id', 'event', 'target', 'rank', 'access', 'is_mentor', 'place', 
           'address', 'day', 'start', 'end', 'latitude', 'longitude', 'abstract']]
df.sort_values(by='rank', inplace=True)
df.to_csv('data/' + OUT, index=False, sep='|')

