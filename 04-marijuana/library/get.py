import collections
import math
import datetime
import re
import pickle


def get_date(l_articles, index):
    l_date = l_articles[index][u'response'][u'docs'][index]['pub_date'].split('T')[0].split('-')
    date_str = ''.join(l_date)
    date_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
    date_date_plus_one = str(date_date + datetime.timedelta(days=1))
    output = re.sub('-', '', date_date_plus_one)
    return output


def get_1000_articles(api_name, query, start):
    pickle_list = []
    for i in range(0, 100):
        pickle_list.append(api_name.search(q=query, begin_date=start, sort='oldest', page=i + 1))
    first_date, last_date = get_date(pickle_list, 0), get_date(pickle_list, -1)
    with open('data/marijuana_' + first_date + '_to_' + last_date + '.pickle', 'wb') as f:
        pickle.dump(pickle_list, f)
    return pickle_list


def convert(data):
    '''
    this function encodes dictionary of unicode entries into utf8
    from http://stackoverflow.com/questions/1254454/fastest-way-to-convert-a-dicts-keys-values-from-unicode-to-str
    '''
    if isinstance(data, basestring):
        return str(data)
    elif isinstance(data, collections.Mapping):
        return dict(map(convert, data.iteritems()))
    elif isinstance(data, collections.Iterable):
        return type(data)(map(convert, data))
    else:
        return data


def get_nyt_article_stats(articles_and_meta):
    '''
    returns the number of hits, number of hits in 100 pages, and hits per page
    '''
    num_hits = articles_and_meta['response']['meta']['hits']  # total number of articles for query
    hits_per_query_set = articles_and_meta['response']['meta']['offset']  # each query gets up to 100 pages
    hits_per_page = len(articles_and_meta['response']['docs'])  # hits per page
    pages = hits_per_query_set / hits_per_page
    queries = int(math.ceil(num_hits / float(hits_per_page)))
    return num_hits, hits_per_query_set, pages, hits_per_page, queries


def get_date(l_articles, index):
    l_date = l_articles[index][u'response'][u'docs'][index]['pub_date'].split('T')[0].split('-')
    date_str = ''.join(l_date)
    date_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
    date_date_plus_one = str(date_date + datetime.timedelta(days=1))
    output = re.sub('-', '', date_date_plus_one)
    return output


def get_last_date_plus_one(articles_and_meta):
    """
    returns last (not necessarily most recent) date
    """
    date_li = articles_and_meta['response']['docs'][-1]['pub_date'].split('T')[0].split('-')
    date_str = ''.join(date_li)
    date_date = datetime.datetime.strptime(date_str, '%Y%m%d').date()
    date_date_plus_one = str(date_date + datetime.timedelta(days=1))
    output = re.sub('-', '', date_date_plus_one)
    return output


def extract_to_mongod(query, date_begin, date_end, mdb):
    """
    pings nyt api and writes to mongodb
    """
    data_converted = None
    while data_converted is None:
        try:
            data = api.search(q=query, begin_date=date_begin, end_date='20160430', sort='oldest')
            data_converted = convert(data)  # convert unicode to strings
        except:
            pass
        date_date = datetime.datetime.strptime(date_begin, '%Y%m%d').date()
        date_date_plus_one = str(date_date + datetime.timedelta(days=1))
        date_begin = re.sub('-', '', date_date_plus_one)

    stats = get_nyt_article_stats(data_converted)  # outputs key stats from first ping
    pings = stats[-1]  # number of pings required
    pings_list = range(0, pings - 1)
    d_begin = date_begin
    for ping in pings_list:
        print d_begin

        # get data from api
        try:
            data2 = api.search(q=query, begin_date=d_begin, end_date='20160430', sort='oldest')
            data_converted2 = convert(data2)  # convert unicode to strings
            last_date_plus_one = get_last_date_plus_one(data_converted2)
            mdb.insert_one(data_converted2)  # insert one set of articles into db
            d_begin = last_date_plus_one  # update date
        except:
            date_date = datetime.datetime.strptime(d_begin, '%Y%m%d').date()
            date_date_plus_one = str(date_date + datetime.timedelta(days=1))
            d_begin = re.sub('-', '', date_date_plus_one)

    return 'success'


def pickle_mongo(collection_name, filename):
    cur = collection_name.find()
    l = []
    for doc in cur:
        l.append(doc)
    file_name = filename + '.pickle'
    with open(file_name, 'wb') as f:
        pickle.dump(l, f)



