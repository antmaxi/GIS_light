"""
    For pixels in subarea of raster get IDs of intersecting tiles-municipalities and areas of their intersections
"""
import os
import sys
import subprocess
import platform
import pandas as pd
import shutil
import logging
import time
import csv
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--debug', type=bool, help='to run in debug (small) mode or not', default=False)
parser.add_argument('--save_layer', type=str, help='to save the created layer or not', default=False)
parser.add_argument('--save_name', type=str, help='name to save', default="point.sh")
parser.add_argument('--lockdown_file', type=str, help='from which .csv take dates and nuts/comm_id', default=None)
import concurrent.futures
import gc
from csv import writer

from qgis_utils import *


def main(args):
    ##############################################################
    #                    INITIALIZATION
    ##############################################################

    folder_save = os.path.join(os.getcwd(), "dist")
    folder_tiles = os.path.join(os.getcwd(), "pixel", "tiles")

    #  set logging level
    if args.debug:
        logging.basicConfig(format='%(message)s',
                            level=logging.DEBUG)  # choose between WARNING - INFO - DEBUG
    else:
        logging.basicConfig(format='%(message)s',
                            level=logging.INFO)
    logger = logging.getLogger(__name__)

    ##############################################################
    #                       START WORK
    ##############################################################
    ts = [time.time()]
    #crs_name = "epsg:6933"
    crs_name = "epsg:4326"  # in degrees, "epsg:6933" in meters (projective coord. syst)
    tiles_filename = "COMM_RG_01M_2016_" + crs_name.split(":")[-1] + "_fixed" + ".shp"
    # file with all countries' tiles
    path_to_tiles_layer = os.path.join(folder_tiles, tiles_filename)

    # read .csv for processing
    if args.lockdown_file is not None:
        df = pd.read_csv(args.lockdown_file)
    else:
        pass
        #df = pd.read_csv(os.path.join(folder_tiles, "COMM_RG_01M_2016_6933_fixed.shp"))
    save_name = None
    code = "BE"
    save_name = os.path.join(folder_tiles, code + ".shp")
    nuts_yes = [code,]
    nuts_no = []
    comm_yes = []
    comm_no = []
    with QGISContextManager():
        #pixel_polygon = create_pixel_area(100, 200, tile_size_x=1, tile_size_y=1,
        #                                  geom_type="polygon", )
        expr = expression_from_nuts_comm(nuts_yes, nuts_no, comm_yes, comm_no)
        # layer = layer_from_filtered_tiles(path_to_tiles_layer, expr=expr, crs_name="epsg:6933")
        layer, extents, _ = layer_from_filtered_tiles(path_to_tiles_layer, expr=expr, crs_name="epsg:4326",
                                                      save_flag=True, save_name=save_name, get_extent=True)
    if 0:
        with QGISContextManager():
            l = load_layer(path_to_tiles_layer)
            expr = expression_from_nuts_comm(nuts_yes, nuts_no, comm_yes, comm_no)
            #layer = layer_from_filtered_tiles(path_to_tiles_layer, expr=expr, crs_name="epsg:6933")
            layer, extents, _ = layer_from_filtered_tiles(path_to_tiles_layer, expr=expr, crs_name="epsg:4326",
                                              save_flag=True, save_name=save_name, get_extent=True)
            print(extents)
            print("ended")
            dis = []
            comm_ids = []
            times = [time.time(),]
            a = 41500
            k = 10
            j = 8000
            rows = []
            if 0:
                # iterate over pixels and get their distances to the selected tiles
                for i in range(a, a+k):
                    target_layer = create_pixel_area(i, j, geom_type="point", transform=True,
                                                     save_layer=args.save_layer, save_name=args.save_name,)
                    #print(target_layer)
                    params = {  'DISCARD_NONMATCHING' : False,
                                'FIELDS_TO_COPY' : [],  # TODO: add the closest municipality?
                                'INPUT' : target_layer,  # pixel
                                'INPUT_2' : layer,
                                'MAX_DISTANCE' : None, 'NEIGHBORS' : 1, 'OUTPUT' : 'TEMPORARY_OUTPUT', 'PREFIX' : '' }

                    tiles_joined = processing.run('native:joinbynearest', params,
                                   )["OUTPUT"]
                    for feature in tiles_joined.getFeatures():
                        dis.append(feature["distance"])
                        comm_ids.append(feature["COMM_ID"])
                    rows.append([i, j, "{:.4f}".format(feature["distance"])])
                delete_layers()
                print(f"{time.time()-times[0]} s")
                print(f"{dis} m")
                print(comm_ids)


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)