import numpy as np
import statsmodels.api as sm
import pandas as pd


def countries_regressions(df):
    df_cols = df.columns.values
    countries = [str(x) for x in df.index.values]
    years_lights_proto = df_cols[:34]
    years_lights = []

    for year in years_lights_proto:
        years_lights.append(year[3:])

    return_list = []
    for country in countries:
        country_lights = df.ix[country][0:34].tolist()
        country_list = []
        for idx, year in enumerate(years_lights):
            gdp = (df.ix[country][34:])[year]
            zipper = year, country_lights[idx], gdp
            country_list.append(zipper)

        Xy = (np.asarray(country_list).astype(float).T)[1:]
        X = Xy[0]
        y = Xy[1]
        observations = (X.shape[0], 1)
        ones = np.ones(observations)

        X = np.reshape(X, (X.shape[0], 1))
        X = np.hstack((X, ones))
        y = np.reshape(y, (y.shape[0], 1))
        print country
        try:
            model = sm.OLS(y, X, missing='drop').fit()
            country_tuple = (model.params[0], model.params[1], model.rsquared, model.rsquared_adj, model.pvalues[0], model.pvalues[1], model.conf_int()[0][0], model.conf_int()[0][1], model.conf_int()[1][0], model.conf_int()[1][1])
        except:
            country_tuple = (0, 0, 0, 0, 0, 0, 0, 0, 0)
        return_list.append(country_tuple)
        countries_series = pd.Series(countries, name='country')
        return_df = pd.DataFrame(return_list, columns=['beta', 'intercept', 'r', 'r_adj', 'p_beta', 'p_int', 'c_beta_low', 'c_beta_high', 'c_int_low', 'c_int_high']).join(countries_series).set_index('country')
    return return_df
