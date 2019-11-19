from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import numpy as np
import pandas as pd

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

#once we get a big dataset this should only be run once 
#this is the equivalent of fit
def fitldaModel(comments, num_topics=15, n_jobs=6):
    '''this takes in a dataframe that has a column named "comments" and creates an LDA model
    and returns it for further use. Num_topics is the number of topics to use and n_jobs is
    the number of cores on CPU to use. n_jobs cannot be -1'''
    
    #clean and tokenize the data a little
    def tokenize(data):
        comm=data.lower()
        comm=re.sub(r'[^a-zA-Z ^0-9]', '', comm)
        stops=[x for x in STOPWORDS]
        stops= stops + [' ']
        return [token for token in comm.split(' ') if token not in stops]
    comments['tokens']=comments['comment'].apply(tokenize)
    
    #set up dictionary
    id2word=corpora.Dictionary(comments['tokens'])
    
    #prune dictionary
    id2word.filter_extremes(no_below=2)
    
    #create the corpus
    corpus=[id2word.doc2bow(text) for text in comments['tokens']]
    
    #create and run the LDA Model
    lda = LdaMulticore(corpus=corpus,
                   id2word=id2word,
                   random_state=723812,
                   num_topics = num_topics,
                   passes=10,
                   workers=n_jobs
                  )
    
    return lda, id2word