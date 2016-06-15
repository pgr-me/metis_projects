import urllib2
from bs4 import BeautifulSoup
import pickle


# obtained data from https://tariffs.ib-net.org


def get_utility_ids(path):
    with open(path, 'r') as f:
        reader = f.read()
    soup = BeautifulSoup(reader, 'lxml')
    soup_list = soup.find_all('a', href=True)
    ids_list = []
    for idx, some_soup in enumerate(soup_list):
        utility_id = soup_list[idx]['href'].encode('latin-1', errors='replace').split('tariffId=')[1].split('&countryId=0')[0]
        ids_list.append(utility_id)
    return list(set(ids_list))


def get_data(utility_ids, url_begin, url_end):
    for an_id in utility_ids:
        url = url_begin + an_id + url_end
        page = urllib2.urlopen(url).read()
        soup = BeautifulSoup(page, 'lxml')
        path = 'data/utilities/' + an_id + '.pickle'
        with open(path, 'wb') as f:
            pickle.dump(soup, f)
