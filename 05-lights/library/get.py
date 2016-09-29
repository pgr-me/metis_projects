import os


def mkdir(data_dir):
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
