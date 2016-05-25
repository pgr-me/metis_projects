loaimport pickle
import glob


def get_filenames_with_same_ext(directory, extension_name):
    l = []
    for path in glob.glob(directory + '/*.' + extension_name):
        l.append(path)
    return l


def load_pickle(path):
    with open(path) as f:
        loaded_pickle = pickle.load(f)
    return loaded_pickle


def append_pickled_lists(directory, extension, path_to_write_to):
    l = []
    pickle_names = get_filenames_with_same_ext(directory, extension)
    for p in pickle_names:
        loaded_pickle = load_pickle(p)
        pickled_list = pickle_to_list(loaded_pickle)
        l.append(pickled_list)
    with open(path_to_write_to, 'wb') as f:
        pickle.dump(l, f)


def pickle_to_list(loaded_pickle):
    pickled_list = []
    for i in loaded_pickle:
        first_level = i[u'response'][u'docs']
        for j in first_level:
            temp = []
            try:
                an_id = j[u'_id']
                temp.append(an_id)
            except:
                an_id = 'none'
                temp.append(an_id)
            try:
                pub_date = j[u'pub_date']
                temp.append(pub_date)
            except:
                pub_date = 'none'
                temp.append(pub_date)
            try:
                org = j[u'byline'][u'organization']
                temp.append(org)
            except:
                org = 'none'
                temp.append(org)
            try:
                headline = j[u'headline'][u'main']
                temp.append(headline)
            except:
                headline = 'none'
                temp.append(headline)
            try:
                lead = j[u'lead_paragraph']
                temp.append(lead)
            except:
                lead = 'none'
                temp.append(lead)
            pickled_list.append(temp)
    return pickled_list


def flatten_list(nested_list, path_to_write_to):
    flat_list = []
    for i in nested_list:
        for j in i:
            flat_list.append(j)
    with open(path_to_write_to, 'wb') as f:
        pickle.dump(flat_list, f)
