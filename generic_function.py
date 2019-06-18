import json
import logging
import os

import folium
import geopandas as gpd
import numpy as np
import pandas as pd
from geoalchemy2 import Geometry, WKTElement
import sqlalchemy
from fiona.crs import from_epsg
from shapely.geometry import Point
from sqlalchemy import create_engine

# from advanced_script import raster_processing
from unitary_tests import unitary_tests

""" Global variable """
# formatting the console outputs
logging.basicConfig(level=logging.INFO, format='%(asctime)s -- %(levelname)s -- %(message)s')

db_name = param["prod_connexion"]["db_name"]
username = param["prod_connexion"]["username"]
password = param["prod_connexion"]["password"]
host = param["prod_connexion"]["host"]
port = param["prod_connexion"]["port"]
prod_conn = "postgresql://{}:{}@{}:{}/{}".format(username, password, host, port,
                                                      db_name)

""" Functions for reading /writing with Postgis  """


def import_table(table_name, con):
    """ Read Postgis Table and return GeoDataFrame

    :param table_name: schema.table_name
    :type con: sqlalchemy.Engine
    """

    gdf = gpd.GeoDataFrame.from_postgis("SELECT * FROM " + table_name, con, geom_col='geom')
    gdf.crs = {'init': 'epsg:2154'}

    assert type(gdf) == gpd.geodataframe.Geodataframe, "the output file in not a GeoDataFrame"
    return gdf


def polygon_to_multipolygon(gdf):
    """ Transform GeoDataFrame Polygon Geometry to MultiPolygon """

    for index in gdf.index:
        if gdf.geometry[index].type == 'Polygon':
            gdf.geometry.loc[index] = MultiPolygon([gdf.geometry.loc[index]])

    return gdf

def multipolygon_to_polygon(gdf):
    """ Transform GeoDataFrame (Surface) Geometry to simple Polygon - Deagregator """

    gdf_singlepoly = gdf[gdf.geometry.type == 'Polygon']
    gdf_multipoly = gdf[gdf.geometry.type == 'MultiPolygon']

    for i, row in gdf_multipoly.iterrows():
        series_geometries = pd.Series(row.geometry)
        df = pd.concat([gpd.GeoDataFrame(row, crs=gdf_multipoly.crs).T] * len(series_geometries), ignore_index=True)
        df['geometry'] = series_geometries
        gdf_singlepoly = pd.concat([gdf_singlepoly, df])

    gdf_singlepoly.reset_index(inplace=True, drop=True)
    return gdf_singlepoly

def write_output(gdf, table_name, schema, conn):
    """ Write GeoDataFrame in PostGis Table / execute some unitary tests

    :type gdf: GeoDataFrame (geometry column = "geometry")
    :param table_name : name of the output Postgis table
    :param schema: name of the output Postgis schema
    :type conn: sqlalchemy.Engine
    """

    export = gdf.copy()
    gdf["area"] = gdf.geometry.apply(lambda x: x.area)
    geometry = gdf.geometry[gdf.index.min()].geom_type.upper()

    # transform geometry to WKT
    export['geometry'] = export['geometry'].apply(lambda x: WKTElement(x.wkt, srid=2154))
    export.rename(columns={'geometry': 'geom'}, inplace=True)

    export.to_sql(table_name, conn, schema=schema, if_exists='append', index=False,
                  dtype={'geom': Geometry(geometry, srid=2154)})
    logging.info("Writing table {}.{} Over".format(schema, table_name))

    # unitary tests
    assert unitary_tests.compare_count_gdf_vs_postgis(gdf, schema + "." + table_name,
                                                      conn), "Number of entities is different after writing the table"
    assert unitary_tests.compare_area_gdf_vs_postgis(gdf, schema + "." + table_name,
                                                     conn), "Area of entities is different after writing the table"
    assert unitary_tests.check_sql_duplicate_geometry(schema + "." + table_name,
                                                      conn), "We found duplicate geometry in urban_project table"
    assert unitary_tests.check_null_geometry(schema + "." + table_name,
                                             conn), "We found null geometry in urban_project table"
    assert unitary_tests.check_duplicate_uuid(schema + "." + table_name,
                                              conn), "We found duplicate uuid in urban_project table"
    assert unitary_tests.check_null_name(schema + "." + table_name,
                                         conn), "We found null urban project name in urban_project table"


def execute_sql_request(ch_sql_request, sql_file, new_value, con):
    """  Open a template sql file containing request whose names to modify
         replacing the name with a new value
         & execute the sql request

         :param ch_sql_request: path to sql folder
         :param sql_file: name of the sql file
         :param new_value: value replacement value
         :type con: sqlalchemy.Engine

    """

    logging.info("Execute a sql request")
    full_ch_sql_request = ch_sql_request + "/" + sql_file + ".sql"
    with open(full_ch_sql_request, "r") as sql_request:
        rqt = sql_request.read().decode('utf-8-sig')

    rqt = rqt.replace('TABLE_NAME', new_value)
    con.execute(rqt)

    # possibility to retrieve the result of the request with a variable = con.execute(rqt).scalar()


def creation_table(ch_table, conn, schema, table_name, username):
    """
    Create empty table from a sql file

    :param ch_table: full path to CREATE TABLE sql file
    :type conn: sqlalchemy.Engine
    :param schema: name of the output Postgis schema
    :param table_name:name of the output Postgis table
    :param username: username for the Postgis connexion
    """

    existing_request = "SELECT EXISTS ( SELECT 1 FROM information_schema.tables WHERE table_schema = '{}'" \
                       "AND table_name = '{}' );".format(schema, table_name)
    schema_exist = conn.execute(existing_request).scalar()

    if schema_exist is False:
        with open(ch_table, "r") as sql_file:
            rqt = sql_file.read().replace('NEW_TABLE_NAME', schema + "." + table_name).replace(
                'all_rights_DATABASE_NAME', 'all_rights_' + table_name).replace('DATABASE_USERNAME', username)
            conn.execute(rqt)
            logging.info('Creation new table - end ')
    else:
        logging.info("Table {} already exist".format(output_table_name))


""" Function for work with shapefile """


def formatting_gdf_for_shp_export(gdf, output_path, output_name):
    """ Formatting GeoDataFrame for export & export to shp

     :type gdf: GeoDataFrame
     :param output_path: complete path for the shapefile export
     :param output_name: name for the shapefile export

     """
    logging.info('formatting & export GeoDataFrame')
    for gdf_column in gdf.columns:
        if type(gdf[gdf_column][0]) == np.bool_:
            gdf[gdf_column] = gdf[gdf_column].astype(str)
        if type(gdf[gdf_column][0]) == pd._libs.tslib.Timestamp:
            gdf[gdf_column] = gdf[gdf_column].astype(str)
        if len(gdf_column) > 10:
            gdf = gdf.rename(columns={gdf_column: gdf_column[:10]})

    gdf.to_file(output_path + "/" + output_name + '.shp')


def geom_to_wkb(gdf):
    """ Transform a GeoDataFrame.geometry to WKB for writing in Postgis Table"""
    gdf['geom'] = gdf['geom'].apply(lambda x: WKTElement(x.wkt, srid=2154))
    return gdf


""" Filling or formatting Functions """


def isolate_duplicate_row(gdf, field):
    """ Isolate duplicated values in specified field and return unique value (Geo)DataFrame & isolate (Geo)DataFrame

    :type gdf: GeoDataFrame
    :param field: name of the column use for identify the duplicate
    :type field: str or unicode
    :return gdf: gdf without duplicate rows
    :return gdf_duplicate_value: gdf with duplicate rows only
    """

    gdf_duplicate_value = gdf.loc[gdf[field].duplicated(keep='first') == True]
    gdf = gdf.loc[gdf[field].duplicated(keep='first') == False]

    return gdf, gdf_duplicate_value


def create_index(gdf):
    """ Increment index if ther are null or not_unique """

    if gdf.index.isnull().sum() > 0 or gdf.index.is_unique == False:
        gdf.index = range(1, len(table) + 1)
    return gdf


def clean_gdf_by_geometry(gdf):
    """ Clean a GeoDataFrame : drop null / invalid / empty geometry """

    logging.info("drop null & invalid & duplicate geometry")
    # reset index for avoid geometry series
    gdf = gdf.reset_index()

    # Check geometry validity
    invalid_geometry = gdf[gdf.geometry.is_valid == False].count().max()
    if invalid_geometry > 0:
        gdf = gdf[gdf.geometry.is_valid == True]
        logging.info("We found and drop {} invalid geometry".format(invalid_geometry))
        logging.warning("these buildings will not be integrated into the PostGis output table")

    # check empty geometry
    null_geometry = gdf[gdf.geometry.is_valid == True].count().max()
    if null_geometry > 0:
        gdf = gdf[gdf.geometry.is_empty == False]

    # Check duplicates geometry
    unique_geometry = gdf.geometry.astype(str).nunique()
    number_duplicate_geometry = gdf.geometry.count() - unique_geometry

    if unique_geometry != gdf.geometry.count():
        wkb_geometry = gdf["geometry"].apply(lambda geom: geom.wkb)
        gdf = gdf.loc[wkb_geometry.drop_duplicates().index]

    logging.info("We found and drop {} duplicates geometry".format(number_duplicate_geometry))
    assert unique_geometry == gdf.geometry.count(), "Geometry problem in the input data: the deleted entity" \
                                                    "number is greater than the duplicate entity number"

    # re-initialization of the indexes in relation to the identifiers
    gdf.index = gdf.id
    return gdf


def drop_col(gdf, list_cols):
    """
    Drop a list a determine columns

    :type gdf: GeoDataFrame
    :type list_cols: list
    :param: list of columns to drop
    """

    gdf = gdf[gdf.columns[gdf.columns.isin(list_cols)]]
    return gdf


""" Spatial operation """


def find_nearest_neighbors(gdf, table_name, con):
    """ Finding the nearest neighbors and recover height (a attribute - exemple)

    :type gdf: GeoDataFrame
    :type table_name: str
    :type con: sqlalchemy.Engine

    """
    logging.info("find nearest neighbors")
    centroid = gdf.geometry.centroid
    centroid = centroid.apply(lambda x: 'SRID=2154;' + str(x))
    centroid = centroid.to_frame(name='geometry')
    gdf['height'] = 0

    for centroid_index in centroid.index:
        try:
            rqt_sql = "SELECT height::int FROM {} ORDER BY {}.geom" \
                      "<-> ('{}'::geometry) LIMIT 1;".format(table_name, table_name,
                                                             str(centroid["geometry"][centroid_index]))
            gdf.height[centroid_index] = con.execute(rqt_sql).scalar()
        except sqlalchemy.exc.InternalError as sqlalchemy_error:
            logging.error(sqlalchemy_error)
            logging.error("index failed : centroid_index")

    assert gdf.height.isna().sum() == 0, "All buildings have no height"
    return gdf


def elevation_recovery_from_dem(gdf):
    """ Find elevation for building gdf
        Warning : the raster_processing need parameters (not informed) in this function"""

    logging.info("recover elevation from DEM ")
    gdf = advanced_script.raster_processing.GetRasterValueOnGeometry(gdf).gdf
    gdf = gdf.rename(columns={'raster_value': "elevation"})

    assert gdf.elevation.isna().sum() == 0, "All buildings have no elevation"
    return gdf


def geocode_df(df, latitude_field, longitude_field, epsg):
    """
    Transform a DataFrame to GeoDataFrame based on x, y field

    :type df: DataFrame
    :type latitude_field: Series
    :type longitude_field: Series
    :type epsg: integer
    :return: GeoDataFrame (epsg : epsg)
    """

    logging.info("Geocode Xls")

    geometry = [Point(xy) for xy in zip(df[longitude_field], df[latitude_field])]
    crs = {'init': 'epsg:' + str(epsg)}
    df = df.drop(columns=[longitude_field, latitude_field])

    gdf = gpd.GeoDataFrame(df, crs=crs, geometry=geometry)
    return gdf


def select_data_in_territory(gdf, gdf_territory):
    """
    Select data in specified area with sjoin with the use of spatial index
    Warning : in this example, gdf com from shapefile (geometry field = geometry)
         and gdf_territory come from Postgis Table (geometry field = geom)

    :param gdf: GeoDataFrame to filter
    :param gdf_territory: GeoDataFrame use for filter
    :return: filtered GeoDataFrame
    """

    logging.info("select data in territory")
    # Intersection with Rtree
    spatial_index = gdf.geometry.sindex
    for i, bounds in enumerate(gdf_territory.bounds.iterrows()):
        possible_matches_index = list(spatial_index.intersection(bounds[1]))
        possible_matches = gdf.iloc[possible_matches_index]
        precise_matches_index = possible_matches.intersects(gdf_territory.iloc[i].geom)
        precise_matches_index = precise_matches_index[precise_matches_index].index
        gdf = gdf.loc[precise_matches_index]


def find_hole_in_polygon_building(gdf):
    """
    Find hole polygon in GeoDataFrame (Polygon)
    :type gdf: GeoDataFrame
    :return: GeoDataFrame with hole polygon only
    """

    logging.info('finding hole in building')
    temporary_gdf_building = gdf.copy()
    temporary_gdf_building.geometry = gdf.exterior
    temporary_gdf_building.geometry = temporary_gdf_building.geometry.apply(lambda x: Polygon(x))

    gdf_hole = gdf[temporary_gdf_building.geometry != gdf.geometry]
    gdf_hole.crs = {'init': 'epsg:2154'}
    simple_part = round((float(gdf_hole.count().max()) / float(gdf.count().max()) * 100), 5)

    gdf_hole['area_building'] = gdf_hole.geometry.apply(lambda x: x.area)
    logging.info(
        "We found {} buildings with hole in the territory, which represents {} % of total building number".format(
            gdf_hole.count().max(), simple_part, origin))

    return gdf_hole


def geocode_with_api(path_to_rpls_csv):
    """
    Use the government geocoder for geocoding csv file
    This exemple base to the RPS file (for the localization of HLM building)

    :type path_to_rpls_csv: csv file (with columns list in geocode_rqt)
    :return df_hlm: DataFrame with x & y columns
    """

    logging.info("START geocoding")

    geocode_rqt = "curl -X POST -F data=@{} -F columns=NUMVOIE -F columns=INDREP -F columns=TYPVOIE -F " \
                  "columns=NOMVOIE -F columns=CODEPOSTAL -F columns=LIBCOM  https://api-adresse" \
                  ".data.gouv.fr/search/csv/".format(path_to_rpls_csv)
    logging.info("execute this request : " + geocode_rqt)

    result_geocoding = subprocess.check_output(geocode_rqt)
    result_geocoding = result_geocoding.decode('utf_8_sig')
    result_geocoding = result_geocoding.encode('utf_8_sig')

    with open('output/result_geocoding.csv', "w") as output_csv:
        output_csv.write(result_geocoding)
    df_hlm = pd.read_csv(ch_dir + "/output/result_geocoding.csv", sep=';')
    return df_hlm


def convert_3d_to_2d(geometry):
    """ Tranform 3D geometry (from GeoDataFrame.Series) to 2D geometry """

    new_geo = []
    for p in geometry:
        if p.has_z:
            if p.geom_type == 'Polygon':
                ac = mapping(p)["coordinates"]
                shell = [(x, y) for x, y, z in ac[0]]
                holes = []
                for h in ac[1:]:
                    hole = [(x, y) for x, y, z in h]
                    holes.append(hole)
                new_p = Polygon(shell=shell, holes=holes)
                new_geo.append(new_p)
            elif p.geom_type == 'MultiPolygon':
                new_multi_p = []
                for ac in p:
                    ac = mapping(p)["coordinates"]
                    shell = [(x, y) for x, y, z in ac[0]]
                    holes = []
                    for h in ac[1:]:
                        hole = [(x, y) for x, y, z in h]
                        holes.append(hole)
                    new_p = Polygon(shell=shell, holes=holes)
                    # new_p = Polygon(shell=ac[0], holes=ac[1:])
                    new_multi_p.append(new_p)
                new_geo.append(MultiPolygon(new_multi_p))
            elif p.geom_type == 'LineString':
                lines = [xy[:2] for xy in list(p.coords)]
                new_p = LineString(lines)
                new_geo.append(new_p)
            elif p.geom_type == 'Point':
                points = [xy[:2] for xy in list(p.coords)]
                new_p = Point(points)
                new_geo.append(new_p)
    return new_geo

""" Leaflet plugin """


def initialize_interactive_map(gdf):
    """
    Generate and initialize the folium file : the view level and the localisation, list of base layer /

    :type gdf: GeoDataFrame - polygon or MultiPolygon
    """

    logging.info("Create & initialize interactive map")
    assert type(gdf) == gpd.geodataframe.GeoDataFrame, 'Out_Territory is not a GeoDataFrame'

    max_min = gdf['geometry'].to_crs(epsg='4326').total_bounds
    moy_y = (max_min[0] + max_min[2]) / 2
    moy_x = (max_min[1] + max_min[3]) / 2

    interactive_map = folium.Map(location=[moy_x, moy_y], zoom_start=13)
    list_base_map = ["Stamen Terrain", "Stamen Toner", "OpenStreetMap"]
    for base_map_name in list_base_map:
        folium.TileLayer(base_map_name).add_to(interactive_map)

    return interactive_map


def folium_add_data_with_popup(gdf, name, color, interactive_map):
    """
    Add layer to interactive map (create by initialize_interactive_map())

    :type gdf: GeoDataFrame (areal) -- possibility to modify the code for import Point or Line
    :param name: name of the output layer in the final interactive_map
    :param color: HEX color use for the layer representation
    :param interactive_map: folium.map  -- result of initialize_interactive_map()
    """

    logging.info("Add data to interactive map")
    assert type(gdf) == gpd.geodataframe.GeoDataFrame, 'Out_Territory is not a GeoDataFrame'

    geojson = gdf.to_crs(epsg='4326').to_json()
    fcolor = lambda feature: dict(fillColor=color, color='#000000', weight=1, fillOpacity=0.9)
    nom_col = []
    alias_col = []
    for i in gdf.columns:
        if (i != 'geom') and (i != 'fc_arrays') and (i != 'geometry'):
            nom_col.append(str(i))
            alias_i = str(i) + ' : '
            alias_col.append(alias_i)
    geojson_map = folium.features.GeoJson(geojson, name=name, style_function=fcolor,
                                          tooltip=folium.features.GeoJsonTooltip(fields=nom_col, aliases=alias_col))
    interactive_map.add_child(geojson_map)
    return interactive_map


def finalize_export_interactive_map(interactive_map):
    """
        Export the interactive map create by the two functions above
        In this exemple, the path and the output name is determined
    """

    logging.info("Export interactive map")
    folium.LayerControl().add_to(interactive_map)
    interactive_map.save(ch_loc + '/synthse_carto.html')
