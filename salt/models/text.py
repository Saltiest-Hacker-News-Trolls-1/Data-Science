import re
import string, itertools
from gensim import corpora
from gensim.utils import simple_preprocess
from gensim.parsing.preprocessing import STOPWORDS

# preserves puncutation
def tokenize(text):
    text += ' ' # cutting off the last token for some reason
    text = text.lower()
    separators = string.punctuation + string.whitespace
    separators_re = "|".join(re.escape(x) for x in separators if x not in {'@'})
    tokens = zip(re.split(separators_re, text), re.findall(separators_re, text))
    flattened = itertools.chain.from_iterable(tokens)
    # keep stopwords, Words like 'but' contribute meaningfully in terms of negativity
    cleaned = [x for x in flattened if x and not x.isspace()]
    return cleaned

# comments is a series
def get_tokens(comments):
    tokens_col = comments.apply(tokenize)
    return tokens_col