import fiona
import geopandas as gpd
import glob
from rasterstats import zonal_stats
import pandas as pd


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
