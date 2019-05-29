# -*- coding: utf-8 -*-
"""
Created on Wed Jan 23 16:58:01 2019

@author: bdaniere


"""


from shapely.geometry import Polygon
from shapely.geometry import LineString
from shapely.geometry import Point
from shapely.geometry import box

import json
import numpy as np
import rasterio

"""
Global variables
"""
json_param = open("param.json")
param = json.load(json_param)



"""
Classes and functions
"""

class GetRasterValueOnGeometry(object):
    """
    Class : GetRasterValueOnGeometry

    """

    def __init__(self, gdf_building):

        self.no_data = -99999
        self.vector_data = gdf_building
        self.raster = param["Sub_data"]["MNT_Territory"]
        self.mode = 'min'
        self._get_raster_value_on_geometry()

    def _get_raster_value_on_geometry(self):
        """
        _get_raster_value_on_geometry

        """

        # filter by raster bounds
        raster_opened = rasterio.open(self.raster)
        raster_bounds = raster_opened.bounds
        study_area = box(
            raster_bounds.left,
            raster_bounds.right,
            raster_bounds.bottom,
            raster_bounds.top
        )
        self.vector_data = self.vector_data[
            self.vector_data.geometry.intersects(study_area)
        ]

        # get raster value on each geom
        self.vector_data = self.vector_data.copy()

        self.vector_data['raster_value'] = self.vector_data.geometry.apply(
            lambda x: self._rast_value_query_func(x, raster_opened))

    def _rast_value_query_func(self, geometry, raster):
        """
        _rast_value_query_func

        :type geometry: shapely.geometry
        :type raster_src: rasterio
        """

        coordinates = []
        if isinstance(geometry, Point):
            coordinates = geometry.coords.xy
            coordinates = [(coordinates[0][-1], coordinates[-1][-1])]
        if isinstance(geometry, Polygon):
            geometries = self._points_gridding(geometry)
            if len(geometries) > 0:
                coordinates = [
                    (value.coords.xy[0][-1], value.coords.xy[-1][-1])
                    for value in geometries
                ]
            else:
                coordinates = geometry.centroid.coords.xy
                coordinates = [(coordinates[0][-1], coordinates[-1][-1])]

        if len(coordinates) == 0:
            coordinates = geometry.representative_point.coords.xy
            coordinates = [(coordinates[0][-1], coordinates[-1][-1])]

        values = [value for value in raster.sample(coordinates)]

        values = [x for x in values if x != raster.profile['nodata']]

        if len(values) > 0:
            value = self._compute_mode(values)
        else:
            value = self.no_data

        return value

    def _compute_mode(self, values):
        """
        _compute_mode

        :param values:
        :type values:
        """

        if self.mode == 'min':
            return min(values[0])
        elif self.mode == 'max':
            return max(values[0])
        elif self.mode == 'avg':
            return sum(values[0]) / len(values[0])

    def _points_gridding(self, geometry):
        """
        _points_gridding

        :param geometry:
        :type geometry:
        """
        interval = LineString([
            (geometry.bounds[0], geometry.bounds[1]),
            (geometry.bounds[2], geometry.bounds[3])]
        ).length / 2

        x_interval = interval
        y_interval = interval
        ll = geometry.bounds[:2]
        ur = geometry.bounds[2:]

        low_x = int(ll[0]) / x_interval * x_interval
        upp_x = int(ur[0]) / x_interval * x_interval + x_interval
        low_y = int(ll[1]) / y_interval * y_interval
        upp_y = int(ur[1]) / y_interval * y_interval + y_interval

        output = [
            Point(x, y)
            for x in np.arange(low_x, upp_x, x_interval)
            for y in np.arange(low_y, upp_y, y_interval)
            if Point(x, y).within(geometry)
        ]

        return output

    @property
    def gdf(self):
        """
        gdf

        """
        return self.vector_data
