
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
parser.add_argument('--debug', type=str, help='to run in debug (small) mode or not', default="False")
parser.add_argument('--rewrite_result', type=bool,
                    help='whether to rewrite or rather append the resulting csv/xlsx file', default=False)
parser.add_argument('--tilename', type=str, help='name of the file with tiles to get from',
                    default="COMM_RG_01M_2016_4326_fixed.shp")
parser.add_argument('--result_name', type=str, help='name of the file with tiles', default="pixels")
parser.add_argument('--country', type=str, help='name of the country to process', default=None)
parser.add_argument('--exact_sizes', type=bool, help='whether input is exact sizes of pixels to process or '
                                                     'it should be calculated',
                    default=None)
# TODO make argument - list for pixels
parser.add_argument('--x0', type=int, help='xmin pixel', default=None)
parser.add_argument('--x1', type=int, help='xmax pixel', default=None)
parser.add_argument('--y0', type=int, help='ymin pixel', default=None)
parser.add_argument('--y1', type=int, help='ymax pixel', default=None)
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

from qgis_utils import *

def debug_output_with_time(st, ts, logger):
    logger.debug(st)
    ts.append(time.time())
    logger.debug(ts[-1] - ts[-2])


def main(args):
    ##############################################################
    #                    INITIALIZATION
    ##############################################################
    folder = os.path.join(os.getcwd(), "label")
    folder_tiles = os.path.join(os.getcwd(), "pixel", "tiles")
    args.tilename = os.path.join(folder_tiles, args.tilename)
    result_format = "csv"

    check_intersection = True

    if check_intersection:
        result_name = args.result_name
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

    country = args.country
    if country != "None" and args.tilename != "None":
        tiles = os.path.join(folder, "tiles", args.tilename + ".shp")
    else:
        tiles = os.path.join(folder, "tiles", "COMM_RG_01M_2016_4326.shp")  # map of municipalities

    # get pixels' area which to process now
    if 0: #not args.exact_sizes:
        # (end_x - start_x_0) // args.n should be divided by pixel_sizes[0]
        start_x = start_x_0 + (end_x - start_x_0) // args.n * args.id_x
        end_x = start_x_0 + (end_x - start_x_0) // args.n * (args.id_x + 1)
        # (end_y - start_y_0) // args.n should be divided by pixel_sizes[0]
        start_y = start_y_0 + (end_y - start_y_0) // args.m * args.id_y
        end_y = start_y_0 + (end_y - start_y_0) // args.m * (args.id_y + 1)
    else:
        (start_x, end_x, start_y, end_y) = (args.x0, args.x1, args.y0, args.y1)

    #  set logging level
    if args.debug == "True":
        print("Debug")
        logging.basicConfig(format='%(message)s',
                            level=logging.DEBUG
                            )# choose between WARNING - INFO - DEBUG
    else:
        logging.basicConfig(format='%(message)s',
                            level=logging.WARNING)# DEBUG)
    logger = logging.getLogger(__name__)
    ##############################################################
    #                       START WORK
    ##############################################################
    start = time.time()
    # if 0: #args.rewrite_result:  # args.rewrite_result:
    #     if os.path.exists(result_name):
    #         os.remove(result_name)
    #     with open(result_name, "w+", newline='') as file:
    #         filewriter = csv.writer(file, delimiter=",")
    #         filewriter.writerow(result_header)
    count = 0
    with QGISContextManager() as qgis_manager:
        global_count = 0
        #  iterate over pixels in rectangular zone of input raster
        times = [time.time()]

        #print(sys.argv)
        tiles = load_layer(args.tilename)
        global_count = get_intersect_ids_and_areas(int(args.x0), int(args.y0), tiles, result_name, global_count,
                                                   tile_size_x=pixel_sizes[0],
                                                   tile_size_y=pixel_sizes[0],
                                                   metric=qgis_manager.metric,
                                                   level=0, pixel_sizes=pixel_sizes,
                                                   logger=logger,
                                                   )
    end = time.time()
    print("Elapsed time {} minutes".format((end - start) / 60.0))
    return 0


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)