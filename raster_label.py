import os
import sys
import subprocess
import shutil
import logging
import time
import csv

from csv import writer
import pandas as pd

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
)
from qgis.utils import iface
from qgis.analysis import QgsNativeAlgorithms

from qgis import processing
from processing.core.Processing import Processing
import processing


def get_intersect_ids_and_areas(i, j, raster_file, tiles, temp, tile_size_x=1, tile_size_y=1,
                                level=None, pixel_sizes=None, check_intersection=False, LOG=None, remove_all=True):
    '''

    :param i:
    :param j:
    :param raster_file:
    :param tiles:
    :param temp:
    :param tile_size_x:
    :param tile_size_y:
    :return:
    '''

    if remove_all:
        os.makedirs(temp, exist_ok=True)
    out_name = os.path.join(temp, ("tile_" + str(i) + "_" + str(j) + ".tif"))

    # TODO: keep all intermediate files in memory (but prevent overfloating as before when tried),
    #  so that no I/O and directory removal needed

    # get one pixel from raster by its i and j pixel coordinates
    com_string = "gdal_translate -of GTIFF -q -srcwin " + str(i) + ", " + str(j) + ", " + str(
        tile_size_x) + ", " + str(tile_size_y) + " " + raster_file + " " + out_name
    os.system(com_string)

    # polygonize pixel
    polygon_filename = "tile_" + str(i) + "_" + str(j)
    subprocess.run(["gdal_polygonize.py", out_name, temp, polygon_filename])
    # load polygonized pixel back
    pixel_polygon = QgsVectorLayer(os.path.join(temp, (polygon_filename + ".shp")), "Polygon layer", "ogr")
    if not pixel_polygon.isValid():
        print("Layer failed to load!")
    else:
        QgsProject.instance().addMapLayer(pixel_polygon)

    if not check_intersection:
        # get area of the pixel only, without intersection
        d = QgsDistanceArea()
        d.setEllipsoid('WGS84')
        elem = pixel_polygon.getFeatures()[0] # one feature
        area = d.convertAreaMeasurement(d.measureArea(elem.geometry()), QgsUnitTypes.AreaSquareKilometers)
        return [[i, j, area]]
    else:
        # intersect polygonized pixel with tiles
        params = {
            "INPUT": tiles,
            "INTERSECT": pixel_polygon,
            'OUTPUT': 'memory:buffer',
            "PREDICATE": [0]
        }
        tiles_intersect = processing.run('qgis:extractbylocation', params,
                                         )["OUTPUT"]

        # get intersections tiles
        params = {
            "INPUT": tiles_intersect,
            "OVERLAY": pixel_polygon,
            "OUTPUT": 'memory:buffer'
        }
        layer = processing.run("qgis:intersection", params,
                               )["OUTPUT"]
        LOG.info("Intersected with pixel")

        # get IDs of intersection tiles
        feature_ids = [feature["COMM_ID"] for feature in layer.getFeatures()]
        rows = [[]]
        if feature_ids:
            if tile_size_x != 1:
                if len(feature_ids) == 1:
                    rows = []
                    for x in range(0, tile_size_x):
                        for y in range(0, tile_size_y):
                            rows.append([i, j, feature_ids[0], -1, 100.0])
                    return rows
                else:
                    pass
            else:
                d = QgsDistanceArea()
                d.setEllipsoid('WGS84')
                areas = []
                k = 0
                for k, feature in enumerate(layer.getFeatures()):
                    # get area of intersection in km^2
                    areas.append(d.convertAreaMeasurement(d.measureArea(feature.geometry()), QgsUnitTypes.AreaSquareKilometers))
                for ind in range(k+1):
                    rows.append([i, j, feature_ids[ind], areas[ind], areas[ind]/sum(areas)*100.0])

            # remove loaded layers
            for l in (layer, pixel_polygon):
                QgsProject.instance().removeMapLayers([l.id()])
            layerList = QgsProject.instance().layerTreeRoot().findLayers()
            for layer in layerList:
                print(layer.name())
            if remove_all:
                shutil.rmtree(temp, ignore_errors=True)
        return rows


def main():
    ##############################################################
    #                    INITIALIZATION
    ##############################################################
    qgis_path = "/home/anton/anaconda3/envs/arcgis/bin/qgis"

    folder = r"/home/anton/Documents/GIT/RA/pixel"
    temp = os.path.join(folder, "temp")

    raster_name = r"SVDNB_npp_d20190329.rade9d.tif"  # light raster map
    raster_file = os.path.join(folder, raster_name)
    tiles = os.path.join(folder, "tiles", "COMM_RG_01M_2016_4326.shp")  # map of municipalities

    result_format = "csv"
    result_name = os.path.join(folder, "pixel_labels_areas." + result_format)

    log_pixel = os.path.join(folder, "log_pixel.txt")
    rewrite_result = True
    result_writer_freq = 5

    remove_all = True

    check_intersection = False

    tile_size_x = 1
    tile_size_y = 1

    # Calculation of needed area:
    #   approx Europe x: -12 - 41, y: 30 - 75
    #          raster x: -180 - 180, y: -65 - 75
    # size of raster 86401 x 33601
    # => needed pixels approx x: 40300 - 53000 , y (map and raster coords are opposite): 0 - 10800
    # total approx 137 millions of pixels
    start_x = 48000  #  40300  #
    end_x = 53000

    start_y = 0  # 6000 #  0  #
    end_y = 12000  # 10800

    # logging level set to INFO
    logging.basicConfig(format='%(message)s',
                        level=logging.WARNING)

    LOG = logging.getLogger(__name__)
    ##############################################################
    #                   INITIALIZE QGIS
    ##############################################################
    # Supply path to qgis install location
    QgsApplication.setPrefixPath('/usr', True)
    QgsApplication.setPrefixPath(qgis_path, True)
    # Create a reference to the QgsApplication.  Setting the second argument to False disables the GUI.
    qgs = QgsApplication([], False)
    # Load providers
    qgs.initQgis()

    sys.path.append('/usr/share/qgis/python/plugins')
    Processing.initialize()
    ##############################################################
    #                       START WORK
    ##############################################################
    start = time.time()
    ds = gdal.Open(str(raster_file))
    band = ds.GetRasterBand(1)

    xsize = 2 #band.XSize
    ysize = 2 #band.YSize

    LOG.info("Size of initial raster {} x {}".format(band.XSize, band.YSize))
    if check_intersection:
        result_header = ['X', 'Y', 'COMM_ID', 'AREA', 'AREA_PERCENT']
    else:
        result_header = ['X', 'Y', 'AREA']
    if rewrite_result:
        if os.path.exists(result_name):
            os.remove(result_name)
        with open(result_name, "w+", newline='') as file:
            filewriter = csv.writer(file, delimiter=",")
            filewriter.writerow(result_header)
    count = 0
    result_buffer = []
    pixel_sizes = [400, 80, 20, 5, 1]
    #  iterate over pixels in rectangular zone of input raster
    for i in range(start_x, end_x):  #start_x + xsize, tile_size_x):
        LOG.info("!!!!!!!!!!!!!!!!")
        LOG.info(i)
        LOG.info("!!!!!!!!!!!!!!!!")
        # get empty directory for vectorized pixels
        shutil.rmtree(str(temp), ignore_errors=True)
        os.makedirs(temp, exist_ok=True)
        for j in range(start_y, end_y):  #start_y + ysize, tile_size_y):

            result_rows = get_intersect_ids_and_areas(i, j, raster_file, tiles, temp, tile_size_x=1, tile_size_y=1,
                                                level=0, pixel_sizes=pixel_sizes, check_intersection=check_intersection,
                                               LOG=LOG, remove_all=remove_all)
            result_buffer.append(result_rows)
            count += 1

            # Append rows to csv file
            LOG.info("{} {}".format(i, j))
            for row in result_rows:
                LOG.info(row)
            with open(log_pixel, "a+") as f:
                f.write("\n {} {}".format(i, j))
            if not count % result_writer_freq:
                print(f"Speed {(time.time() - start) / count}")
                with open(result_name, "a+", newline="") as file:
                    filewriter = csv.writer(file)
                    for rows in result_buffer:
                        for row in rows:
                            if row:
                                filewriter.writerow(row)
                result_buffer = []

    # drop remained data
    with open(result_name, "a+", newline="") as file:
        filewriter = csv.writer(file)
        for rows in result_buffer:
            for row in rows:
                if row:
                    filewriter.writerow(row)
    # Finally, exitQgis() is called to remove the provider and layer registries from memory
    QgsProject.instance().removeAllMapLayers()
    qgs.exit()

    end = time.time()
    print("Elapsed time {} minutes".format((end - start) / 60.0))


if __name__ == '__main__':
    main()
