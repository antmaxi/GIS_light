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
parser.add_argument('-n', type=int,  help='number of programs to run in parallel, divide x-axis', default=1)
parser.add_argument('-m', type=int,  help='number of subprograms to run consequently, divide y-axis', default=1)
parser.add_argument('--id_x', type=int,  help='id of the program run on x-axis', default=0)
parser.add_argument('--id_y', type=int,  help='id of the program run on y-axis', default=0)
parser.add_argument('--debug', type=bool, help='to run in debug (small) mode or not', default=False)
parser.add_argument('--rewrite_result', type=bool,
                    help='whether to rewrite or rather append the resulting csv/xlsx file', default=False)
parser.add_argument('--tilename', type=str, help='name of the file with tiles', default="France")
parser.add_argument('--result_name', type=str, help='name of the file with tiles', default="pixels")
parser.add_argument('--country', type=str, help='name of the country to process', default=None)
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
    QgsPointXY,
    QgsGeometry,
    QgsFeature,
    # QgsMapLayerRegistry
)
from qgis.utils import iface
from qgis.analysis import QgsNativeAlgorithms

from qgis import processing
from processing.core.Processing import Processing
import processing


def debug_output_with_time(str, ts, LOG):
    LOG.debug(str)
    ts.append(time.time())
    LOG.debug(ts[-1] - ts[-2])


def delete_layers():
    if 1:
        for l in QgsProject.instance().mapLayers().values():
            QgsProject.instance().removeMapLayer(l.id())
        for l in QgsProject.instance().mapLayers().values():
            print(l.name())
    return 0

def create_pixel_area(i, j, tile_size_x, tile_size_y, get_area_only=False, metric=None,
                      step=0.004166666700000000098, x0=-180.0020833333499866, y0=75.0020833333500008):

    x2 = 180.0020862133499975
    y2 =  -65.0020844533500082
    x_size = 86401
    y_size = 33601

    p1 = QgsPointXY(x0+i*step, y0-j*step)
    p2 = QgsPointXY(x0+(i+tile_size_x)*step, y0-j*step)
    p3 = QgsPointXY(x0+(i+tile_size_x)*step, y0-(j+tile_size_y)*step)
    p4 = QgsPointXY(x0+i*step, y0-(j+tile_size_y)*step)
    points = [p1, p2, p3, p4]
    feat = QgsFeature()
    feat.setGeometry(QgsGeometry.fromPolygonXY([points]))
    if get_area_only:
        return metric.convertAreaMeasurement(metric.measureArea(feat.geometry()), QgsUnitTypes.AreaSquareKilometers)
        #QgsProject.instance().removeMapLayer(layer.id())
    else:
        layer = QgsVectorLayer('Polygon?crs=epsg:4326', 'polygon' , 'memory')
        prov = layer.dataProvider()
        prov.addFeatures([feat])
        layer.updateExtents()
        QgsProject.instance().addMapLayers([layer])

        return layer

def get_intersect_ids_and_areas(i, j, raster_file, tiles, temp, result_name, global_count,
                                tile_size_x=None, tile_size_y=None,
                                metric=None,
                                level=None, pixel_sizes=None, check_intersection=True, LOG=None, remove_all=True,
                                path_to_gdal=None):
    '''
    :param level:
    :param i:
    :param j:
    :param raster_file:
    :param tiles:
    :param temp:
    :param tile_size_x:
    :param tile_size_y:
    :return:
    '''
    LOG.info("Processing i={} j={} size={}".format(i, j, tile_size_x))
    #if not os.path.exists(temp):
    #    os.makedirs(temp, exist_ok=True)
    #out_name = os.path.join(temp, ("tile_" + str(i) + "_" + str(j) + "_" + str(tile_size_x) + ".tif"))

    # TODO: keep all intermediate files in memory (but prevent overfloating as before when tried),
    #  so that no I/O and directory removal needed
    ts = [time.time()]
    # get one square area from raster by its i and j pixel coordinates and sizes
    pixel_polygon = create_pixel_area(i, j, tile_size_x, tile_size_y, metric=metric)

    debug_output_with_time("Polygonized area", ts, LOG)

    if not check_intersection:
        # get area of the pixel only, without intersection
        d = QgsDistanceArea()
        d.setEllipsoid('WGS84')
        for elem in pixel_polygon.getFeatures():
            area = d.convertAreaMeasurement(d.measureArea(elem.geometry()), QgsUnitTypes.AreaSquareKilometers)
            return [[i, j, area]]
    else:
        # intersect polygonized pixel with tiles
        #tiles_intersect = os.path.join(temp, ("tile_" + str(i) + "_" + str(j) + "_" + str(tile_size_x) + "_inter.shp"))
        params = {
            "INPUT": tiles,
            "INTERSECT": pixel_polygon,
            'OUTPUT': 'memory:buffer', #tiles_intersect,  #
            "PREDICATE": [0]
        }
        tiles_intersect = \
        processing.run('qgis:extractbylocation', params,
                           )["OUTPUT"]

        debug_output_with_time("Found intersection tiles", ts, LOG)
        # get intersections tiles
        #layer = os.path.join(temp, ("tile_" + str(i) + "_" + str(j) + "_" + str(tile_size_x) + "_int_tiles"))
        params = {
            "INPUT": tiles_intersect,
            "OVERLAY": pixel_polygon,
            "OUTPUT": 'memory:buffer' #layer,  #
        }
        layer_intersect = \
            processing.run("qgis:intersection", params,
                           )["OUTPUT"]

        debug_output_with_time("Intersected tiles with pixel", ts, LOG)

        # delete polygonized area layer
        for l in (pixel_polygon, tiles_intersect):
            QgsProject.instance().removeMapLayer(l.id())


        #if tile_size_x > 1:
        # get IDs of intersection tiles
        feature_ids = [feature["COMM_ID"] for feature in layer_intersect.getFeatures()]
        feature_nuts = [feature["NUTS_CODE"] for feature in layer_intersect.getFeatures()]
        rows = []  #  result accumulation
        # if there is some intersection with municipalities' tiles
        if len(feature_ids) > 0:
            # single pixel or aggregation of pixels?
            if tile_size_x != 1:
                # is the whole area inside one municipality or not
                if len(feature_ids) == 1:
                    # get area of every pixel inside and append to results
                    for x in range(0, tile_size_x):
                        for y in range(0, tile_size_y):
                            rows.append([i + x, j + y, feature_nuts[0], feature_ids[0],
                                         "{:.6f}".format(create_pixel_area(i+x, j+y, 1, 1,
                                                                           get_area_only=True,metric=metric)),
                                                         100.0])
                    debug_output_with_time(f"Done with area of size {tile_size_x}", ts, LOG)
                    global_count += tile_size_x * tile_size_y
                # area is divided between municipalities
                else:
                    n = pixel_sizes[level] // pixel_sizes[level + 1]
                    size = pixel_sizes[level + 1]  # current size of "aggregated pixel"
                    for k in range(n):
                        for m in range(n):
                            LOG.debug(f"{i} {j} {size} {k} {m}")
                            global_count = get_intersect_ids_and_areas(i + k * size, j + m * size,
                                                                        raster_file, tiles, temp, result_name,
                                                                        global_count,
                                                        tile_size_x=size, tile_size_y=size, metric=metric,
                                                        level=level + 1, pixel_sizes=pixel_sizes,
                                                        check_intersection=True,
                                                        LOG=LOG, remove_all=remove_all,
                                                        path_to_gdal=path_to_gdal)
            # single pixel
            else:  # tile_size_x == 1
                areas = []
                k = 0
                for k, feature in enumerate(layer_intersect.getFeatures()):
                    # get area of intersection in km^2
                    areas.append(
                        metric.convertAreaMeasurement(metric.measureArea(feature.geometry()),
                                                 QgsUnitTypes.AreaSquareKilometers))
                for ind in range(k + 1):
                    rows.append([i, j, feature_nuts[ind], feature_ids[ind],
                                 "{:.6f}".format(areas[ind]), "{:.4f}".format((areas[ind] / sum(areas)) * 100.0)])
                debug_output_with_time("Calculated area of intersections", ts, LOG)
                global_count += 1
            # dump obtained results
            with open(result_name, "a+", newline="") as file:
                filewriter = csv.writer(file)
                for row in rows:
                    if row:
                        filewriter.writerow(row)
                        LOG.debug(row)
                        #print(row)
            debug_output_with_time("Dumped results", ts, LOG)
            # remove loaded layers
            delete_layers()
            gc.collect()
            debug_output_with_time("Deleted all layers", ts, LOG)

        return global_count


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
        assert "Not Windows or Linux, not guaranteed to work"

    folder = os.path.join(os.getcwd(), "pixel")
    #temp = os.path.join(folder, "temp" + "_" + str(args.id_x) + "_" + str(args.id_y))

    raster_name = r"SVDNB_npp_d20190329.rade9d.tif"  # light raster map
    raster_file = os.path.join(folder, raster_name)

    result_format = "csv"

    check_intersection = True

    if check_intersection:
        result_name = args.result_name # os.path.join(folder, args.result_name
                                   #+ "." + result_format)
        result_header = ['X', 'Y', 'NUTS_CODE', 'COMM_ID', 'AREA', 'AREA_PERCENT']
        pixel_sizes = [
            40,
            8,
            2,
            1]
    else:
        result_name = os.path.join(folder, "pixel_areas." + result_format)
        result_header = ['X', 'Y', 'AREA']
        pixel_sizes = [1]

    log_pixel = os.path.join(folder, "log_pixel.txt")
    result_writer_freq = 1

    remove_all = False

    country = args.country #"France"
    if country:
        tiles = os.path.join(folder, "tiles", args.tilename + ".shp")
    else:
        tiles = os.path.join(folder, "tiles", "COMM_RG_01M_2016_4326.shp")  # map of municipalities
    # Calculation of needed area:
    #   approx Europe x: -12 - 41, y: 30 - 75
    #          raster x: -180 - 180, y: -65 - 75
    # (exactly: -180.0020833333499866,-65.0020844533500082 : 180.0020862133499975,75.0020833333500008
    # pixel size 0.004166666700000000098,-0.004166666700000000098 )

    # size of raster 86401 x 33601
    # => needed pixels approx x: 40300 - 53000 , y (map and raster coords are opposite): 0 - 10800
    # total approx 137 millions of pixels
    #print(args.country)
    if 1: #not args.debug:
        if country == "France":  # -5.3 x 10, 41 x 51.5 => in pixels: 42400 - 45600,
            start_x_0 = 41960
            end_x =  45640  # difference 40*92
            start_y_0 = 5720
            end_y = 8120  # difference 40*60
        elif country == "Spain":
            start_x_0 = 40880
            end_x = 44400  # difference 40*88
            start_y_0 = 7480
            end_y = 9720  # difference 40*56
        elif country == "Spain_islands":
            start_x_0 = 38780
            end_x = 40060  # difference 40*32
            start_y_0 = 10880
            end_y = 11360  # difference 40*12
        elif country == "UK":
            start_x_0 = 41120
            end_x = 43680  # difference 40*64
            start_y_0 = 3360
            end_y = 6080  # difference 40*68
        elif country == "Germany":
            start_x_0 = 44600
            end_x = 46840 # difference 40*64
            start_y_0 = 3360
            end_y = 6080  # difference 40*68
        elif country == "Italy":
            start_x_0 = 44790
            end_x = 47670  # difference 40*72
            start_y_0 = 6640
            end_y = 9520  # difference 40*72
        elif country == "whole":
            start_x_0 = 40600
            end_x = 53000
            start_y_0 = 0
            end_y = 10800
        else:
            raise NotImplementedError

    else:
        start_x_0 = 48000
        end_x = 48520
        start_y_0 = 6000
        end_y = 6520
    # (end_x - start_x_0) // args.n should be divided by pixel_sizes[0]
    start_x = start_x_0 + (end_x - start_x_0) // args.n * args.id_x
    end_x = start_x_0 + (end_x - start_x_0) // args.n * (args.id_x + 1)
    # (end_y - start_y_0) // args.n should be divided by pixel_sizes[0]
    start_y = start_y_0 + (end_y - start_y_0) // args.m * args.id_y
    end_y = start_y_0 + (end_y - start_y_0) // args.m * (args.id_y + 1)

    #  set logging level
    if args.debug:
        logging.basicConfig(format='%(message)s',
                            level=logging.WARNING)  # choose between WARNING - INFO - DEBUG
    else:
        logging.basicConfig(format='%(message)s',
                            level=logging.WARNING)# DEBUG)
    LOG = logging.getLogger(__name__)
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
    start = time.time()
    #LOG.info("Size of initial raster {} x {}".format(band.XSize, band.YSize))
    #print(args.rewrite_result)
    #print(f"{args.n} {args.m} {args.id_x} {args.id_y} ")
    if 0: #args.rewrite_result:  # args.rewrite_result:
        if os.path.exists(result_name):
            os.remove(result_name)
        with open(result_name, "w+", newline='') as file:
            filewriter = csv.writer(file, delimiter=",")
            filewriter.writerow(result_header)
    count = 0

    # Create a reference to the QgsApplication.  Setting the second argument to False disables the GUI.
    qgs = QgsApplication([], False)
    # Load providers
    qgs.initQgis()
    Processing.initialize()
    # ds = gdal.Open(str(raster_file))
    metric = QgsDistanceArea()
    metric.setEllipsoid('WGS84')

    global_count = 0
    #  iterate over pixels in rectangular zone of input raster
    times = [time.time()]
    for i in range(start_x, end_x, pixel_sizes[0]):
        # get empty directory for vectorized pixels
        #if remove_all:
        #    shutil.rmtree(str(temp), ignore_errors=True)
        #if not os.path.exists(temp):
        #    os.makedirs(temp, exist_ok=True)
        temp = None
        for j in range(start_y, end_y, pixel_sizes[0]):

            global_count = get_intersect_ids_and_areas(i, j, raster_file, tiles, temp, result_name, global_count,
                                        tile_size_x=pixel_sizes[0], tile_size_y=pixel_sizes[0],
                                        metric=metric,
                                        level=0, pixel_sizes=pixel_sizes,
                                        check_intersection=check_intersection,
                                        LOG=LOG, remove_all=remove_all, path_to_gdal=path_to_gdal
                                        )
            print(f"Done big tile {i} {j}")
            count += 1
            times.append(time.time())
            print(f"{time.strftime('%H:%M:%S', time.localtime())}, "
                  f"global speed per 1 Mpixel {((time.time() - start) / count / (pixel_sizes[0] ** 2) * 10 ** 6):.2}s, "
                  f"local speed per 1 Mpixel  {((times[-1] - times[-2]) / (pixel_sizes[0] ** 2) * 10 ** 6):.2}s, "
                  f"processed pixels: {count * pixel_sizes[0] ** 2 // 10 ** 6} M {count * pixel_sizes[0] ** 2 % 10 ** 6 // 10 ** 3} K")

            QgsProject.instance().removeAllMapLayers()
            #if remove_all:
            #    shutil.rmtree(temp, ignore_errors=True)
    # Finally, exitQgis() is called to remove the provider and layer registries from memory
    qgs.exit()
    end = time.time()
    print("Elapsed time {} minutes".format((end - start) / 60.0))
    return 0


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)