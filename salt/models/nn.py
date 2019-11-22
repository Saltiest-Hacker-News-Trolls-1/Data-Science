# Utilities for training the Neural Network

from tensorflow.keras.models import load_model
from tensorflow.keras.preprocessing.sequence import pad_sequences
import text
import pickle
import numpy as np

MAXLEN=32
id2word = None

label_table =(
    {0 : 'no emotion',
     1 : 'anger', 
     2 : 'disgust', 
     3 : 'fear', 
     4 : 'happiness', 
     5 : 'sadness', 
     6 : 'surprise'})

def save_file(file, thing):
    """Pickle thing to file"""
    with open(file, 'wb') as handle:
        pickle.dump(thing, handle, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"[i] Saved to file: {file}")
    
def load_file(file):
    """Load pickle from file"""
    result = None
    with open(file, 'rb') as handle:
        result = pickle.load(handle)
    return result

top_words = load_file('top_words.pickle')
id2word = load_file('id2word.pickle')

def filter_tokens(keep_tokens, token_list):
    """Filter the token list for tokens which are in keep tokens"""
    new_list = []
    for token in token_list:
        if token in keep_tokens:
            new_list.append(token)
           
    return new_list

def get_token_seqs(token_list):
    """Get token sequences from a list that can be fed into the neural network"""
    token_seq = []
    for token in token_list:
        token_seq.append(id2word.token2id[token])
    token_seq = pad_sequences([token_seq], padding='post', truncating='pre', maxlen=MAXLEN)[0]
    return np.array(token_seq)

def doc2token_seq(doc):
    """String to a sequence that can be fed into the neural network"""
    tokens = text.tokenize(doc)
    tokens = filter_tokens(top_words['word'].values, tokens)
    token_seq = get_token_seqs(tokens)
    return token_seq

def predict_text(model, text):
    """Predict a single line of text"""
    token_seq = doc2token_seq(text)
    pred_proba = model.predict(np.array([token_seq]))
    pred = np.argmax(pred_proba)
    print(f'Text: {text}\nPredicted Emotion: {label_table[pred]}')
    
def update_id2word(value):
    global id2word 
    id2word = value
    
def example():
    model = load_model('emote_model')
    predict_text(model, 'Wish me luck!')
    predict_text(model, 'AA I hate everything')
    predict_text(model, 'Excited emotion!!??!')
    predict_text(model, 'Bro you are such an idiot')