"""
    For pixels in subarea of raster get IDs of intersecting tiles-municipalities and areas of their intersections
"""
import glob
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
parser.add_argument('--export_layer', type=str, help='to save the created layer or not', default=False)
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

    folder_save = os.path.join(os.getcwd(), "dist", )
    folder_tiles = os.path.join(os.getcwd(), "pixel", "tiles")
    folder_labels = os.path.join(os.getcwd(), "label", )

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
    # crs_name = "epsg:6933"
    crs_name = "epsg:4326"  # in degrees, "epsg:6933" in meters (projective coord. syst)
    tiles_filename = "COMM_RG_01M_2016_" + crs_name.split(":")[-1] + "_fixed" + ".shp"
    tiles_path = os.path.join(folder_tiles, tiles_filename)
    # file with all countries' tiles
    path_to_tiles_layer = os.path.join(folder_tiles, tiles_filename)

    # read .csv for processing
    if args.lockdown_file is not None:
        df = pd.read_csv(args.lockdown_file)
    else:
        pass
        # df = pd.read_csv(os.path.join(folder_tiles, "COMM_RG_01M_2016_6933_fixed.shp"))
    save_name = None
    # alg_type = "extract_border"
    alg_type = "get_dist_to_border"

    codes = [
         "AT",
         "BE", "CH", "CZ", "DK", "IE", "NL", "PL", "PT",
         "LI", "MC", "SM",
        #"AD",
        # "DE", "FR", "ES", "IT", "UK", "GB"
    ]
    for code in codes:
        if alg_type == "extract_border":
            save_path = os.path.join(folder_tiles, "border_" + code + ".shp")
            if not os.path.exists(save_path):
                with QGISContextManager():
                    tiles_border, _, err = get_border_of_country(code, tiles_path, save_flag=True, save_path=save_path)
        elif alg_type == "get_dist_to_border":
            print(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())} Started {code}")
            with QGISContextManager():
                tiles_layer = load_layer(os.path.join(folder_tiles, "border_" + code + ".shp"))
                # create result file
                result_name = os.path.join(folder_save, "dist_border_" + code + ".csv")
                result_header = ['X', 'Y', 'DIST_BORDER_METERS', "NEAREST_COMM_ID"]
                with open(result_name, "w+", newline='') as file:
                    filewriter = csv.writer(file, delimiter=",")
                    filewriter.writerow(result_header)
                # iterate over files with labeled pixels to get their x and y numbers
                for f in glob.glob(os.path.join(folder_labels, "pixel_label_" + code + "*")):
                    df = pd.read_csv(f, header=0)
                    df = df[["X", "Y"]].drop_duplicates()
                    rows = []
                    # iterate over retrieved pixels and get their distances to the selected tiles
                    for index, row in df.iterrows():
                        i, j = row["X"], row["Y"]
                        rows.append(measure_dist(i, j, tiles_layer, dist_type="point_to_tiles",
                                                 save_point=False)
                                    )
                        if index % 1000 == 0:
                            print(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())} {index}")
                    with open(result_name, "a+", newline="") as file:
                        filewriter = csv.writer(file)
                        for row in rows:
                            if row:
                                filewriter.writerow(row)
        elif alg_type == "get_dist_to_lockdown":
            pass
        print(f"{code} {time.time() - ts[-1]}")
        ts.append(time.time())


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
