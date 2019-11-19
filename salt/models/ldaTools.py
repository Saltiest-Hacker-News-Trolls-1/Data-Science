from gensim.parsing.preprocessing import STOPWORDS
from gensim import corpora

from gensim.models.ldamulticore import LdaMulticore

from ScoringFunctions.py import tokenize


def get_dict_corpus(comments):
    '''takes in a cleaned dataframe of comments with a tokens column
    returns a gensim dictionary object and a corpus of those words'''
    id2word=corpora.Dictionary(comments['tokens'])
    id2word.filter_extremes(no_below=2)
    corpus=[id2word.doc2bow(text) for text in comments['tokens']]
    return id2word, corpus



#once we get a big dataset this should only be run once 
#this is the equivalent of fit
def fitldaModel(comments, num_topics=15, n_jobs=6):
    '''this takes in a dataframe that has a column named "comments" and creates an LDA model
    and returns it for further use. Num_topics is the number of topics to use and n_jobs is
    the number of cores on CPU to use. n_jobs cannot be -1'''
    
    #clean and tokenize the data a little
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