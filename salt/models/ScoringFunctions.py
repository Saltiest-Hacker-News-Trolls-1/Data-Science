from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import numpy as np

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