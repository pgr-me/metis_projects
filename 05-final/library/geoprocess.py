import glob
import pandas as pd
import shutil
import subprocess
import os

import fiona
import geopandas as gpd
from rasterstats import zonal_stats
import ogr
import osr
import rasterio
from rasterio import features
from rasterio.features import shapes
from shapely.geometry import mapping, shape


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


def polygonize(input_tif_dir, output_shp_dir, countries):

    rm_and_mkdir(output_shp_dir)
    for country in countries:
        shp_filename = country + '.shp'
        output_shp_path = os.path.join(output_shp_dir, shp_filename)
        tif_filename = country + '.tif'
        input_tif_path = os.path.join(input_tif_dir, tif_filename)

        with rasterio.open(input_tif_path) as src:
            band = src.read(1)

        mask = band != 255
        shapes = features.shapes(band, mask=mask, transform=src.transform)
        geomvals = list(shapes)

        geom_val_trios = []
        for idx, geom_val in enumerate(geomvals):
            shapely_geom = shape(geomvals[idx][0])
            shapely_val = geomvals[idx][1]
            geom_val_trio = [shapely_geom, shapely_val, country]
            geom_val_trios.append(geom_val_trio)
        gdf = gpd.GeoDataFrame(geom_val_trios, columns={'geometry', 'val', 'country'})
        gdf.crs = {'init': 'epsg:4326', 'no_defs': True}
        gdf.to_file(output_shp_path)


def union_and_filter(input_dir, output_dir, countries):
    # make dir to hold unioned and dissolved shapefiles
    rm_and_mkdir(output_dir)
    for country in countries:
        print country
        # specify io paths
        input_filename = country + '.shp'
        input_path = os.path.join(input_dir, input_filename)
        output_path = os.path.join(output_dir, input_filename)

        # load country shapefile
        gdf_country = gpd.read_file(input_path)
        gdf_country.rename(columns={'country': 'val', 'val': 'country'}, inplace=True)

        # filter out low pixel values
        thresh = 25
        gdf_country = gdf_country[gdf_country['val'] >= thresh]

        # union resulting geometries, assign crs, write to temp file
        polys = gdf_country.geometry
        poly = polys.unary_union
        poly_country = [country, poly]
        gdf_poly_country = gpd.GeoDataFrame(poly_country).T.rename(columns={0: 'country', 1: 'geometry'})
        gdf_poly_country.crs = {'init': 'epsg:4326', 'no_defs': True}
        try:
            gdf_poly_country.to_file(output_path)
        except:
            print 'No polygon values greater than thresh'


def get_countries(the_dir):
    the_path = the_dir + '/*.shp'
    countries = [os.path.basename(x)[:3] for x in glob.glob(the_path)]
    return countries


def split_multi_to_single_poly(input_dir, output_dir):
    # get list of countries in input dir
    countries = get_countries(input_dir)

    # make dir to hold unioned and dissolved shapefiles
    rm_and_mkdir(output_dir)

    for country in countries:
        # specify io directories and filenames
        input_filename = country + '.shp'
        input_path = os.path.join(input_dir, input_filename)
        output_path = os.path.join(output_dir, input_filename)
        # write to split geometries (polys intstead of multi-polys) to target dir
        try:
            print country
            with fiona.open(input_path) as input:
                # create the new file: the driver, crs and schema are the same
                with fiona.open(output_path, 'w', driver=input.driver, crs=input.crs, schema=input.schema) as output:
                    # read the input file
                    for multi in input:
                        # extract each Polygon feature
                        for poly in shape(multi['geometry']):
                            # write the Polygon feature
                            output.write({'properties': multi['properties'], 'geometry': mapping(poly)})
        except:
            print 'error with %s' % country


def merge_shapefiles(input_dir, output_dir, output_filename):
    rm_and_mkdir(output_dir)

    output_path = os.path.join(output_dir, output_filename)

    file_ends_with = '.shp'
    driver_name = 'ESRI Shapefile'
    geometry_type = ogr.wkbPolygon

    out_driver = ogr.GetDriverByName(driver_name)
    out_ds = out_driver.CreateDataSource(output_path)
    out_layer = out_ds.CreateLayer(output_path, geom_type=geometry_type)

    fileList = os.listdir(input_dir)

    for file in fileList:
        if file.endswith(file_ends_with):
            print file
            input_path = os.path.join(input_dir, file)
            ds = ogr.Open(input_path)
            lyr = ds.GetLayer()
            for feat in lyr:
                out_feat = ogr.Feature(out_layer.GetLayerDefn())
                out_feat.SetGeometry(feat.GetGeometryRef().Clone())
                out_layer.CreateFeature(out_feat)
                out_layer.SyncToDisk()
