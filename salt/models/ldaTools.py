from gensim.parsing.preprocessing import STOPWORDS
from gensim import corpora
from gensim.models.coherencemodel import CoherenceModel

from gensim.models.ldamulticore import LdaMulticore
import re
from salt.retriever.tools import query_with_connection
from salt.retriever.log import startLog, getLogFile
import logging

import pandas as pd
import pickle

# startLog(getLogFile(__file__))
RUN_LOG = logging.getLogger('root')
RUN_LOG.info('Connecting to database...')
stops=[x for x in STOPWORDS]
stops= stops + ['', 'im']
    
def tokenize(data):
    ''' this function takes in a string, cleans it and returs it as a list of tokens
    it works as a .apply function on a dataframe'''
    if data is None:
        data = ""
    comm=data.lower()
    comm=re.sub(r'[^a-zA-Z ^0-9]', '', comm)
    return [token for token in comm.split(' ') if token not in stops]

def doc_stream(): 
    users=query_with_connection('''SELECT id FROM users WHERE lda_run='f' LIMIT 3000;''')
    for i, user in enumerate(users):
        RUN_LOG.info(f'selecting from user {i} {user[0]}')
        kids=query_with_connection(f"SELECT text, id FROM items WHERE by='{user[0]}' and lda_salty is NULL LIMIT 100;")
        for comment in kids:
            tokens=tokenize(comment[0])
            # RUN_LOG.info(f'yielding tokens: {tokens}')
            yield tokens, comment[1], user

def update_users(doc_stream, lda, id2word):
    scores=[]
    users=[]
    for comment in doc_stream():
        predictions=predict(comment[0], id2word, lda)
        salt=0
        for score in predictions:
            if score[0]==2:
                salt=float(score[1])
        scores.append({'id':comment[1], 'lda':salt})
        users.append(comment[2])
    return scores, users

def load_data():
    with open('salt/models/LDA_pickle', 'rb') as f:
        lda=pickle.load(f)
    with open('salt/models/Dictionary_pickle', 'rb') as f:
        id2word=pickle.load(f)
    return lda, id2word

def predict(text, id2word, lda, tokens=True):
    if tokens:
        tokens=text
    else:
        tokens=tokenize(text)
    bow=id2word.doc2bow(tokens)
    return lda[bow]

def get_dict_corpus(doc_stream):
    '''takes in a cleaned dataframe of comments with a tokens column
    returns a gensim dictionary object and a corpus of those words'''
    id2word=corpora.Dictionary(doc_stream())
    RUN_LOG.info(f'before filter_extremes len: {len(id2word.keys())}')
    id2word.filter_extremes(no_below=2)
    RUN_LOG.info(f'after filter_extremes len: {len(id2word.keys())}')
    RUN_LOG.info('*********************Done Building Dictionary*********************')
    corpus=[id2word.doc2bow(text[0]) for text in doc_stream()]
    return id2word, corpus

def compute_cv(dictionary, corpus, limit, start=2, step=3, passes=5, n_jobs=6):
    '''
    compute the coherence values for a various number of topics 
    parameters:
    dictionary: a gensim dictionary object
    corpus: a generated gensim corpus obj
    comments: the collection of comments that we're working from 
    limit: max num of topics
    passes: number of times to run the model
    
    returns: coherence_values: a dataframe of the coherence values'''
    
    coherence_values=[]
    
    for iter_ in range(passes):
        RUN_LOG.info(f'starting pass {iter_}')
        for num_topics in range(start, limit, step):
            RUN_LOG.info(f'modeling {num_topics} topics')
            model=LdaMulticore(corpus=corpus,
                               id2word=dictionary,
                               num_topics=num_topics,
                               workers=n_jobs
                               )
            coherence = CoherenceModel(model=model,
                                       dictionary=dictionary, 
                                       corpus=corpus, 
                                       coherence='u_mass')
            coherence_values.append({'pass': iter_,
                                     'num_topics': num_topics,
                                     'coherence_score': coherence.get_coherence()})
    return pd.DataFrame(coherence_values)

def cv_graph(coherenceDF):
    ax=sns.lineplot(x='num_topics',y='coherence_score', data=topic_coherence)
    return ax

#once we get a big dataset this should only be run once 
#this is the equivalent of fit
def fitldaModel(id2word, corpus, num_topics=15, n_jobs=6):
    '''this takes in a dataframe that has a column named "comments" and creates an LDA model
    and returns it for further use. Num_topics is the number of topics to use and n_jobs is
    the number of cores on CPU to use. n_jobs cannot be -1'''
    
    #create and run the LDA Model

    
    return lda


#this function isn't going to be used. I can't bring myself to delete it yet though
def user_means(userComments, id2word, lda):
    '''This function gives the average topic scores for a user.
    userComments should be a list of comments from a single user that you want to get the
    average topic scores for, id2word is a dictionary object, lda is an LDA object'''
    #create the corpus for the current set of comments
    corp=[id2word.doc2bow(tokenize(x)) for x in userComments]
    
    # creates a dense matrix of the topics
    dense=[lda[d] for d in corpus]
    
    #creates a sparse matrix
    def to_sparse(doc):
        d_dist={k:0 for k in range(0,15)} # that big number needs to match number of topics
        for t in doc:
            d_dist[t[0]] = t[1] #the doc will be a dense matrix, pull out the values
        return d_dist
    sparse=[to_sparse(d) for d in dense]
    
    #get the topics
    words = [re.findall(r'"([^"]*)"',t[1]) for t in lda.print_topics()]
    topics = [' '.join(t[0:5]) for t in words]
    
    #create a dataframe of each comment with the user as the index 
    df=pd.DataFrame(sparse)
    df.columns=topics
    
    #return a 1 row dataframe with the mean score for each topic for the user
    return df.mean()