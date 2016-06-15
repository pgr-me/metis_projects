import geopandas as gpd
import pandas as pd
import pickle


# load, clean, and normalize country-level lights data
with open('data/geo/pickles/zonal_stats_c.pickle') as f:
    gdf = pickle.load(f)
gdf = pd.DataFrame(gdf)
gdf = gdf.drop_duplicates(subset='WB_A3')
gdf = gdf.set_index('WB_A3')
gdf.drop(['ADMIN', 'CONTINENT', 'ISO_A3', 'REGION_UN', 'REGION_WB', 'SUBREGION', 'geometry'], axis=1, inplace=True)
gdf_normalizer = (gdf.F101992).as_matrix()
gdf_normed = gdf.divide(gdf_normalizer, axis=0)

# Load, clean, and normalize wb data
wb = pd.read_csv('data/econ/wb.csv')

# wb = wb[wb['Series Name'] == 'GDP at market prices (constant 2005 US$)']
label = 'GDP, PPP (constant 2011 international $)'
wb = wb[wb['Series Name'] == label]
wb.drop(['Country Name', 'Series Name', 'Series Code', '2014', '2015'], axis=1, inplace=True)
wb.rename(columns={'Country Code': 'WB_A3'}, inplace=True)
wb.dropna(axis=0, inplace=True)
wb = wb.set_index('WB_A3')
wb_normalizer = (wb['1992']).as_matrix()
wb_normed = wb.divide(wb_normalizer, axis=0)

# join lights and wb datasets
df = gdf_normed.join(wb_normed, how='inner')

# pickle joined dataframe
df.to_pickle('data/cleaned_df.pickle')