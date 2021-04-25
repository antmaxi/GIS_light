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
parser.add_argument('--save_layer', type=str, help='to save the created layer or not', default=False)
parser.add_argument('--save_name', type=str, help='name to save', default="point.sh")
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
                      get_area_only=False, metric=None, save_layer=False, save_name="layer.sh",
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
    if save_layer:
        options = QgsVectorFileWriter.SaveVectorOptions()
        options.driverName = "ESRI Shapefile"
        if args.save_point:
            error = QgsVectorFileWriter.writeAsVectorFormatV2(target_layer, os.path.join(os.cwd(), args.save_name),
                                                              QgsCoordinateTransformContext(), options, )
            print(error)

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

    folder = os.getcwd() # os.path.join(os.getcwd(), "pixel")

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
    nuts_yes = ["ES11","ES12"]
    nuts_no = ["ES112",]
    comm_yes = ["ES7115902",]
    comm_no = []

    # construct QGIS SQL-like expression to choose tiles
    expr = ""
    att_name = "NUTS_CODE"
    expr = " AND ".join(map(lambda x: "\"" + att_name + "\" LIKE '%" + x + "%'", nuts_yes))
    if expr and nuts_no:
        expr += " AND "
        expr += " AND ".join(map(lambda x: "\"" + att_name + "\" NOT LIKE '%" + x + "%'", nuts_no))
    att_name = "COMM_ID"
    if expr and (comm_yes or comm_no):
        expr += " OR "
    expr += " AND ".join(map(lambda x: "\"" + att_name + "\" = '" + x + "'", comm_yes))
    if expr and comm_no:
        expr += " AND ".join(map(lambda x: "\"" + att_name + "\" <> '" + x + "'", comm_no))
    print(expr)

    # select tiles by NUTS/COMM_ID
    selected_tiles = tiles_layer.getFeatures(QgsFeatureRequest().setFilterExpression(expr))
    # save selected tiles to the new layer
    geom_type = "multipolygon"
    layer_type = geom_type + '?crs=' + crs_name
    layer = QgsVectorLayer(layer_type, geom_type, 'memory')
    prov = layer.dataProvider()
    for feat in selected_tiles:
        prov.addFeatures([feat])
        print(feat["NUTS_CODE"])
        print(feat["COMM_ID"])
    layer.updateExtents()
    QgsProject.instance().addMapLayers([layer])

    dis = []
    times = [time.time(),]
    a = 41500
    k = 100
    j = 8000
    for i in range(a, a+k):
        target_layer = create_pixel_area(i, j, geom_type="point", transform=True,
                                         save_layer=args.save_layer, save_name=args.save_name,)
        #print(target_layer)
        params = {  'DISCARD_NONMATCHING' : False,
                    'FIELDS_TO_COPY' : [],
                    'INPUT' : target_layer,  # pixel
                    'INPUT_2' : layer,
                    'MAX_DISTANCE' : None, 'NEIGHBORS' : 1, 'OUTPUT' : 'TEMPORARY_OUTPUT', 'PREFIX' : '' }

        tiles_joined = processing.run('native:joinbynearest', params,
                       )["OUTPUT"]
        for feature in tiles_joined.getFeatures():
            dis.append(feature["distance"])
        row = [i, j, "{:.4f}".format(feature["distance"])]
    print(f"{time.time()-times[0]} s")
    print(f"{dis} m")
    # Finally, exitQgis() is called to remove the provider and layer registries from memory
    qgs.exit()

if __name__ == '__main__':
    args = parser.parse_args()
    main(args)