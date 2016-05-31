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
    for article in li:
        filtered_words = [word for word in article if word not in stopwords.words('english')]
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
