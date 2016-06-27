#### This is the code for using satellite night imagery to estimate GDP at national and subnational levels

#### Explanation of how to use iPython notebooks
To reproduce the results of the country-level analysis, run iPython notebooks 01 through 03 in order. The city-level regressions that correspond to notebooks 04 through 06 are in progress.

#### This analysis is conducted using Python 2.7, and requires the following Python modules:
* fiona
* geopandas
* glob
* rasterstats
* pandas
* pickle
* sklearn
* statsmodels

#### Notes
Flare lighting for United States, Algeria, Turkmenistan, and Uzbekistan not removed because of geoprocessing issue with data

