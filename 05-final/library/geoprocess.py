import fiona
import geopandas as gpd
import glob
from rasterstats import zonal_stats
import pandas as pd
import shutil
import subprocess
import os


def get_tif_ids(tifs_dir):
    """
    extract id from each tif filename and append each
    id to a list; return list
    """
    glob_str = tifs_dir + '/*.tif'
    li_img = glob.glob(glob_str)
    li_ids = []
    for i in li_img:
        first_chunk = i.split('/')[3]
        second_chunk = first_chunk.split('.')[0]
        li_ids.append(second_chunk)
    return li_ids


def zonal_to_shp(tifs_dir, shp_path):
    """
    calculate zonal statistics of list of tifs in directory
    using countries shapefile
    """
    tifs_glob_str = tifs_dir + '/*.tif'
    tifs = glob.glob(tifs_glob_str)
    ids = get_tif_ids(tifs_dir)
    geodataframe = gpd.GeoDataFrame.from_file(shp_path)
    for idx, tif in enumerate(tifs):
        stats = zonal_stats(shp_path, tif)
        li_stats = []
        for i in range(len(stats)):
            li_stats.append(stats[i]['mean'])
        s_stats = pd.Series(li_stats, name=ids[idx])
        geodataframe = geodataframe.join(s_stats)
    return geodataframe


def rm_and_mkdir(rel_path_to_new_directory):
    # create path to write shapefiles to
    cwd = os.path.abspath('')
    countries_dir = cwd + '/' + rel_path_to_new_directory
    # remove pre-existing directory and shapefiles if exist
    if os.path.exists(countries_dir):
        shutil.rmtree(rel_path_to_new_directory)
    # create directory to write individual country shapefiles to
    if not os.path.exists(countries_dir):
        os.makedirs(countries_dir)


def shp_to_shps(path_to_shp_dir, geodataframe):
    # create new directory
    rm_and_mkdir(path_to_shp_dir)
    # loop through rows of geodataframe and save each row as a country-specific shapefile
    countries = geodataframe.index.values
    for country in countries:
        gdf_country = gpd.GeoDataFrame(geodataframe.loc[country]).transpose()
        path = path_to_shp_dir + '/' + country + '.shp'
        gdf_country.to_file(path)


def raster_to_rasters(countries, input_tif_path, input_shp_dir, output_tif_dir):
    # get current working directory
    cwd = os.path.abspath('')
    # make raster directory
    rm_and_mkdir(output_tif_dir)
    # absolute path to input tif
    input_tif_abs_path = cwd + '/' + input_tif_path
    # loop through countries, clip each country raster, save to output tif directory
    for country in countries:
        input_shp_abs_path = cwd + '/' + input_shp_dir + '/' + country + '.shp'
        output_tif_abs_path = cwd + '/' + output_tif_dir + '/' + country + '.tif'
        # clip raster using country shapefile and save output to tif dir
        subprocess.check_call(['gdalwarp', '-dstnodata', '255', '-q', '-cutline', input_shp_abs_path, '-crop_to_cutline', '-of', 'GTiff', input_tif_abs_path, output_tif_abs_path])
