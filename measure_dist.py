"""
    For pixels in subarea of raster get IDs of intersecting tiles-municipalities and areas of their intersections
"""
import os
import sys
import subprocess
import platform
import shutil
import logging
import time
import csv
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--debug', type=bool, help='to run in debug (small) mode or not', default=False)
import concurrent.futures

import gc

from csv import writer

# QGIS imports
from osgeo import gdal
import qgis

from qgis.core import (
    QgsApplication,
    #    QgsProcessingFeedback,
    QgsVectorLayer,
    QgsProject,
    QgsDistanceArea,
    QgsUnitTypes,
    # QgsMapLayerRegistry
    QgsFeature,
    QgsPointXY,
    QgsGeometry,
    QgsVectorFileWriter,
    QgsFeatureRequest,
    QgsCoordinateTransformContext,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
)
from qgis.utils import iface
from qgis.analysis import QgsNativeAlgorithms

from qgis import processing
from processing.core.Processing import Processing
import processing

#from raster_label import create_pixel_area

def create_pixel_area(i, j, geom_type="polygon", tile_size_x=1, tile_size_y=1,
                      crs_source_name="epsg:4326", transform=False, crs_dest_name="epsg:6933",
                      get_area_only=False, metric=None,
                      step=0.004166666700000000098, x0=-180.0020833333499866, y0=75.0020833333500008):
    # parameters of the raster layer
    x2 = 180.0020862133499975
    y2 =  -65.0020844533500082
    x_size = 86401
    y_size = 33601

    feat = QgsFeature()
    if geom_type == "point":
        # center of square area
        p = QgsPointXY(x0 + (i + tile_size_x / 2.0) * step, y0 - (j + tile_size_y / 2.0) * step)
        if transform:
            sourceCrs = QgsCoordinateReferenceSystem(crs_source_name)
            destCrs = QgsCoordinateReferenceSystem(crs_dest_name)
            tr = QgsCoordinateTransform(sourceCrs, destCrs, QgsProject.instance())
            p = tr.transform(p)
        geom = QgsGeometry.fromPointXY(p)
    elif geom_type == "square":
        if transform:
            raise NotImplementedError(f"No crs transform for polygon yet")
        else:
            p1 = QgsPointXY(x0 + i * step, y0 - j * step)
            p2 = QgsPointXY(x0 + (i + tile_size_x) * step, y0 - j * step)
            p3 = QgsPointXY(x0 + (i + tile_size_x) * step, y0 - (j + tile_size_y) * step)
            p4 = QgsPointXY(x0 + i * step, y0 - (j + tile_size_y) * step)
            points = [p1, p2, p3, p4]
            geom = QgsGeometry.fromPolygonXY([points])
            if get_area_only:
                return metric.convertAreaMeasurement(metric.measureArea(feat.geometry()), QgsUnitTypes.AreaSquareKilometers)
    else:
        raise NotImplementedError(f"Not known type {type}")
    if transform:
        layer_type = geom_type+'?crs=' + crs_dest_name
    else:
        layer_type = geom_type + '?crs=' + crs_source_name
    feat.setGeometry(geom)
    layer = QgsVectorLayer(layer_type, geom_type, 'memory')
    prov = layer.dataProvider()
    prov.addFeatures([feat])
    layer.updateExtents()
    QgsProject.instance().addMapLayers([layer])
    return layer


def delete_layers():
    if 1:
        for l in QgsProject.instance().mapLayers().values():
            QgsProject.instance().removeMapLayer(l.id())
        for l in QgsProject.instance().mapLayers().values():
            print(l.name())
    return 0

def main(args):
    ##############################################################
    #                    INITIALIZATION
    ##############################################################
    if platform.system() == "Linux":
        qgis_path = "/home/anton/anaconda3/envs/arcgis/bin/qgis"
        path_to_gdal = None
    elif platform.system() == "Windows":
        qgis_path = r"C:\Users\antonma\Anaconda3\envs\qgis\Library\python\qgis"
        path_to_gdal = "C:\ProgramData\Anaconda3\envs\qgis\Scripts"
    else:
        qgis_path = "/home/anton/anaconda3/envs/arcgis/bin/qgis"
        assert "Not Windows or Linux, not guaranteed to work"

    folder = os.path.join(os.getcwd(), "pixel")

    #  set logging level
    if args.debug:
        logging.basicConfig(format='%(message)s',
                            level=logging.DEBUG)  # choose between WARNING - INFO - DEBUG
    else:
        logging.basicConfig(format='%(message)s',
                            level=logging.INFO)
    logger = logging.getLogger(__name__)
    ##############################################################
    #                   INITIALIZE QGIS
    ##############################################################
    # Supply path to qgis install location
    QgsApplication.setPrefixPath('/usr', True)
    QgsApplication.setPrefixPath(qgis_path, True)
    sys.path.append('/usr/share/qgis/python/plugins')
    ##############################################################
    #                       START WORK
    ##############################################################

    # Create a reference to the QgsApplication.  Setting the second argument to False disables the GUI.
    qgs = QgsApplication([], False)
    # Load providers
    qgs.initQgis()
    Processing.initialize()
    ts = [time.time()]

    metric = QgsDistanceArea()
    metric.setEllipsoid('WGS84')
    path_to_tiles_layer = os.path.join(folder, "SpainUTM.shp")
    tiles_layer = QgsVectorLayer(path_to_tiles_layer, "Lockdown layer", "ogr")
    if not tiles_layer.isValid():
        print("Layer failed to load!")
    else:
        QgsProject.instance().addMapLayer(tiles_layer)
    crs = tiles_layer.crs()
    logging.info(f"Used CRS: {crs.description()}")
    crs_name = crs.authid()
    crs_name = "epsg:4326"
    crs_name = "epsg:6933"
    nuts = "ES1"
    expr = "\"NUTS_CODE\" LIKE " + "'%" + nuts + "%'"
    print(expr)
    # TODO: select tiles by NUTS/COMM_ID
    #tiles_layer = iface.activeLayer()
    #QgsFeatureRequest
    # selected_tiles = tiles_layer.getFeatures(QgsFeatureRequest().setFilterExpression(expr))
    # geom_type = "multipolygon"
    # layer_type = geom_type + '?crs=' + crs_name
    #
    # layer = QgsVectorLayer(layer_type, geom_type, 'memory')
    # prov = layer.dataProvider()
    # prov.addFeatures([selected_tiles])
    # layer.updateExtents()
    # QgsProject.instance().addMapLayers([layer])
    #selected_tiles = tiles_layer.selectByExpression(expr)["OUTPUT"]
    selected_tiles = r"C:\Users\antonma\RA\sp1.shp"
    # print(layer)
    # print(selected_tiles)
    dis = []
    times = [time.time(),]
    a = 41500
    k = 1
    for i in range(a, a+k):
        target_layer = create_pixel_area(i, 8000, geom_type="point", transform=True,)
        options = QgsVectorFileWriter.SaveVectorOptions()
        options.driverName = "ESRI Shapefile"
        error = QgsVectorFileWriter.writeAsVectorFormatV2(target_layer, r"C:\Users\antonma\RA\sp_pixe9",
                                                  QgsCoordinateTransformContext(), options,)
        print(error)
        print(target_layer)
        params = {  'DISCARD_NONMATCHING' : False,
                    'FIELDS_TO_COPY' : [],
                    'INPUT' : target_layer,  # pixel
                    'INPUT_2' : selected_tiles,
                    'MAX_DISTANCE' : None, 'NEIGHBORS' : 1, 'OUTPUT' : 'TEMPORARY_OUTPUT', 'PREFIX' : '' }

        tiles_joined = processing.run('native:joinbynearest', params,
                       )["OUTPUT"]
        for feature in tiles_joined.getFeatures():
            dis.append(feature["distance"])
    print(time.time()-times[0])
    print(dis)
    # for i in range(20):
    #     for j in range(1000):
    #         area = create_pixel_area(i,i,1,1, get_area_only=False, metric=metric)
    #     #print(area)
    #     ts.append(time.time())
    #     print(ts[-1] - ts[-2])
    path =  ".\playground\saved.shp"
    #QgsVectorFileWriter.writeAsVectorFormat(layer, path, "UTF-8", layer.crs(), 'ESRI Shapefile')
    # Finally, exitQgis() is called to remove the provider and layer registries from memory
    qgs.exit()

if __name__ == '__main__':
    args = parser.parse_args()
    main(args)