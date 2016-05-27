import pandas as pd


def stringify_data(dataframe):
    list_o_cols = dataframe.columns.values
    for col in list_o_cols:
        dataframe[col] = [x.encode('utf-8') for x in dataframe[col]]
    return dataframe


def lower_case_columns(dataframe):
    list_o_cols = dataframe.columns.values
    for col in list_o_cols:
        dataframe['temp'] = dataframe[col].str.lower()
        dataframe.drop(col, axis=1, inplace=True)
        dataframe.rename(columns={'temp': col}, inplace=True)
    return dataframe


def rid_punctuation(dataframe):
    list_o_cols = dataframe.columns.values
    for col in list_o_cols:
        dataframe['temp'] = dataframe[col].str.replace('[^\w\s]', '')
        dataframe.drop(col, axis=1, inplace=True)
        dataframe.rename(columns={'temp': col}, inplace=True)
    return dataframe


def slice_and_dice_col(dataframe, col, start_slice, stop_slice):
    dataframe['temp'] = dataframe[col].str.slice(start=start_slice, stop=stop_slice)
    dataframe.drop(col, axis=1, inplace=True)
    dataframe.rename(columns={'temp': col}, inplace=True)
    return dataframe


def mask(dataframe, key, value, operator='=='):
    if operator == '==':
        return dataframe[dataframe[key] == value]
    if operator == '!=':
        return dataframe[dataframe[key] != value]
    if operator == '>':
        return dataframe[dataframe[key] > value]
    if operator == '>=':
        return dataframe[dataframe[key] >= value]
    if operator == '<':
        return dataframe[dataframe[key] < value]
    if operator == '<=':
        return dataframe[dataframe[key] <= value]


def str_to_date(dataframe, col):
    dataframe['temp'] = pd.to_datetime(dataframe['date_pub'], errors='coerce')


def mask(dataframe, key, value, operator='equals'):
    if operator == 'equals':
        return dataframe[dataframe[key] == value]
    if operator == 'not_equals':
        return dataframe[dataframe[key] != value]


def str_to_date(dataframe, col):
    dataframe['temp'] = pd.to_datetime(dataframe['date_pub'])
    dataframe.drop(col, axis=1, inplace=True)
    dataframe.rename(columns={'temp': col}, inplace=True)
    return dataframe
