from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import numpy as np
import pandas as pd
from gensim.parsing.preprocessing import STOPWORDS
from gensim import corpora

from gensim.models.ldamulticore import LdaMulticore

analyser = SentimentIntensityAnalyzer()

def score_func(text):
    '''returns a dictionary of the scores for a given comment'''
    return analyser.polarity_scores(text)

def get_scores(data):
    '''Returns the mean scores of negative, neutral, positive or combined for 
    a given list of data 
    would make sense to be grouped by user or by article'''
    posScore=[]
    neuScore=[]
    comScore=[]
    negScore=[]
    for comment in data:
        neg, neu, pos, com = analyser.polarity_scores(comment).values()
        negScore.append(float(neg))
        neuScore.append(float(neu))
        posScore.append(float(pos))
        comScore.append(float(com))
    return {'neg': np.mean(negScore), 'neu': np.mean(neuScore), 'pos':np.mean(posScore), 'com':np.mean(comScore)}



def tokenize(data):
    ''' this function takes in a string, cleans it and returs it as a list of tokens
    it works as a .apply function on a dataframe'''
        comm=data.lower()
        comm=re.sub(r'[^a-zA-Z ^0-9]', '', comm)
        stops=[x for x in STOPWORDS]
        stops= stops + ['']
        return [token for token in comm.split(' ') if token not in stops]
    
