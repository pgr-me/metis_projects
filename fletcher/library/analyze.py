import pandas as pd
from textblob import TextBlob
import nltk
from nltk.corpus import stopwords
from sklearn.feature_extraction.text import CountVectorizer
import numpy as np
import math


def bundle_articles(article_list, group_size):
    groups = int(math.ceil(len(article_list) / float(group_size)))
    bundled_articles = []
    for i in range(groups):
        low_index = i * group_size
        high_index = low_index + group_size
        bundled_articles.append(' '.join(article_list[low_index:high_index]))
    return bundled_articles


# export leads as list of strings
def df_to_li(dataframe, col_to_li):
    li = dataframe[[col_to_li]].values.tolist()
    li = [x[0] for x in li]
    return li


def stem_li(li):
    stemmer = nltk.stem.porter.PorterStemmer()
    li_stemmed = []
    for article in li:
        article_temp = []
        for word in TextBlob(article).words:
            stemmed_word = stemmer.stem(word)
            article_temp.append(stemmed_word)
        li_stemmed.append(article_temp)
    return li_stemmed


def remove_stopwords(li):
    li_sw = []
    cached_stop_words = set(stopwords.words('english'))
    cached_stop_words.update(('al', 'a1', '100 report', 'street 212', 'admiss see', 'hour admiss see', 'hour admiss', 'admiss see abov', 'hour admiss see abov', '2495', '2595', 'doubleday', 'metro brief', 'relat articl page', 'relat articl', 'harpercollin', 'littl brown', 'metro brief new', '2795', 'week week list', 'last week week list', 'last week week', 'week thi last', 'week thi last week', 'weak list', 'thi last week', 'thi last week week', 'nation brief', '2396200', '212 2396200', '212', 'street 212 2396200', 'street 212', 'tonight tomorrow', '2396200 brantley', '212 2396200 brantley', 'street clinton', 'clinton 212', 'street clinton 212', 'street 212 2396200 brantley', 'theater', 'tonight tomorrow night', 'dun', 'dargi', '8212', '007', '007 ratti', '007 ratti puni', '007 ratti puni account', '01', '01 nanogram', '01 nanogram per', '01 nanogram per cubic', '011441712405224', '011441712405224 concern', 'tomorrow night', 'said today', 'upi', 'hour admiss', 'admiss see', 'admiss see abov', 'hour admiss see', '99 report', 'hour admiss see above', 'metro brief', 'relat articl page', 'relat articl', 'littl brown', 'metro brief new', 'week list', 'week thi last week', 'thi last week', 'thi last week week', 'last week week', 'week thi last', 'week week list', 'last week week list', 'nation brief', 'brief new york', 'metro brief new york', 'tonight tomorrow night', '3074100', 'brooklyn 718', 'tomorrow night', '44th', '44th street', '718', '99 report'))
    for article in li:
        filtered_words = [word for word in article if word not in cached_stop_words]
        li_sw.append(filtered_words)
    return li_sw


def join_words(li):
    documents = []
    for article in li:
        documents.append(' '.join(article))
    documents = [str(x) for x in documents]
    return documents


def get_tf(documents, max_ngram):
    # CountVectorizer is a class; so `vectorizer` below represents an instance of that object.
    vectorizer = CountVectorizer(ngram_range=(1, max_ngram), encoding='latin-1', decode_error='replace')

    # call `fit` to build the vocabulary
    vectorizer.fit(documents)

    # then, use `get_feature_names` to return the tokens
    # print vectorizer.get_feature_names()

    # finally, call `transform` to convert text to a bag of words
    sparse_matrix = vectorizer.transform(documents)

    x_back = sparse_matrix.toarray()
    df_tf = pd.DataFrame(x_back, columns=vectorizer.get_feature_names())
    return df_tf


def calc_idf(dataframe):
    num_docs = dataframe.shape[0]
    df_count = dataframe.astype(bool).sum(axis=0)
    idf = np.log(num_docs / df_count)
    return idf


def get_tf_idf(df_tf, idf):
    return df_tf * idf
