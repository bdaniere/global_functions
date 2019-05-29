# -*- coding: utf-8 -*-
"""
Created on Wed May 13 2019
@author: bdaniere
"""


def check_urban_project_category(gdf_category, general_category_set):
    not_found_category = gdf_category - general_category_set
    return not_found_category


def compare_count_two_gdf_and_result(gdf1, gdf2, gdf_total):
    count_separate = gdf1.count().max() + gdf2.count().max()
    count_gdf_total = gdf_total.count().max()

    result = count_gdf_total == count_separate
    return result


def compare_count_gdf_vs_postgis(gdf, table_name, conn):
    gdf_count = gdf.count().max()
    sql_count_request = "SELECT count(*) FROM {}".format(table_name)

    table_count = conn.execute(sql_count_request).scalar()
    result = gdf_count == table_count
    return result


def compare_count_gdf_vs_postgis_id_product(gdf, table_name, conn, id_product):
    gdf_count = gdf.count().max()
    sql_count_request = "SELECT count(*) FROM {} WHERE id_product = {}".format(table_name, id_product)

    table_count = conn.execute(sql_count_request).scalar()
    result = gdf_count == table_count
    return result


def compare_area_gdf_vs_postgis(gdf, table_name, conn):
    gdf_sum_area = int(gdf.area.sum())
    sql_count_request = "SELECT sum(ST_Area(geom)) FROM {}".format(table_name)
    table_area = int(conn.execute(sql_count_request).scalar())

    result = gdf_sum_area == table_area
    return result


def compare_area_gdf_vs_postgis_id_product(gdf, table_name, conn, id_product):
    gdf_sum_area = int(gdf.area.sum())
    sql_count_request = "SELECT sum(ST_Area(geom)) FROM {} WHERE id_product = {}".format(table_name, id_product)
    table_area = int(conn.execute(sql_count_request).scalar())

    result = gdf_sum_area == table_area
    return result


def check_sql_duplicate_geometry(building_table_name, conn):
    sql_detect_duplicate_geom = "SELECT count(*) FROM {} as t1, {} as t2 WHERE ST_Equals(t1.geom, t2.geom) AND t1.id != t2.id".format(
        building_table_name, building_table_name)
    duplicate_number = conn.execute(sql_detect_duplicate_geom).scalar()

    return duplicate_number == 0


def check_null_geometry(building_table_name, conn):
    sql_count_null_geom = "SELECT count(*) FROM {} WHERE geom IS NULL".format(building_table_name)
    null_geom = conn.execute(sql_count_null_geom).scalar()

    return null_geom == 0


def check_duplicate_uuid(building_table_name, conn):
    sql_duplicate_uuid = "SELECT count(*) FROM (SELECT count(*) FROM {}  GROUP BY id_src HAVING count(*) > 1) as unique_uuid".format(
        building_table_name)
    count_duplicate_uuid = conn.execute(sql_duplicate_uuid).scalar()

    return count_duplicate_uuid == 0


def check_null_name(building_table_name, conn):
    sql_count_null_geom = "SELECT count(*) FROM {} WHERE name IS NULL".format(building_table_name)
    null_geom = conn.execute(sql_count_null_geom).scalar()

    return null_geom == 0