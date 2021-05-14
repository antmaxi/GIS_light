import datetime
import glob
import math
import pandas as pd

import logging

import argparse

import config as cfg

parser = argparse.ArgumentParser()
parser.add_argument('--lockdown_file', type=str, help='from which .csv take dates and nuts/comm_id', default=None)
parser.add_argument('--load_data', default=False, action='store_true',
                    help='whether to use loaded data or make it from scratch')

parser.add_argument('--debug',  default=False, action='store_true', help='to run in debug (small) mode or not')
parser.add_argument('--export_layer', type=str, help='to save the created layer or not', default=False)
parser.add_argument('--save_name', type=str, help='name to save', default="point.sh")
parser.add_argument('--append', default=False, action='store_true',
                    help='whether to rewrite the result or append')


parser.add_argument('--code', type=str, help='which country code to process', default=None)
parser.add_argument('--alg_type', type=str, help='type of algorithm to run', default=None)

parser.add_argument('--min_row', type=int, help='minimal index of row to process', default=0)
parser.add_argument('--max_row', type=int, help='maximal index of row to process', default=100000000)

from qgis_utils import *


def is_nan(aff):
    if isinstance(aff, str):
        if aff == "":
            return True
    elif isinstance(aff, float):
        if math.isnan(aff):
            return True
    else:
        raise NotImplementedError(f"Not known type of cell {type(aff)}")
    return False


def main(args):
    ##############################################################
    #                    INITIALIZATION
    ##############################################################
    folder_save = cfg.folder_save
    folder_tiles = cfg.folder_tiles
    folder_labels = cfg.folder_labels

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
    crs_name = "epsg:6933"  # "epsg:6933"  #  in meters (projective coord. syst); "epsg:4326"  # in degrees,
    crs_code = crs_name.split(":")[-1]
    tiles_filename = "COMM_RG_01M_2016_" + crs_code + "_fixed" + ".shp"
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
    if args.alg_type is None:
        #args.alg_type = "extract_border"
        args.alg_type = "get_dist_to_border"
        # args.alg_type = "get_dist_to_lockdown"
    if args.code is not None:
        codes = [args.code, ]
    else:
        codes = [
            #"AT",
            #"BE",
            #"CH", "CZ", "DK",
            #"IE", "NL",
            #"PL", "PT",
            #"LI", "MC", "SM",
            #"AD",
            # "DE",
            # "FR", "ES",  "IT", "GB", "ND"
        ]
    for code in codes:
        # code is prefix of COMM_ID for tiles
        print(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())} Started {code}")
        if args.alg_type == "extract_border":
            save_path = os.path.join(folder_tiles, "border_" + code + "_" + crs_code + ".shp")
            with QGISContextManager():
                tiles_border, _, err = get_border_of_country(code, tiles_path, save_flag=True, save_path=save_path,
                                                             crs_name=crs_name, rewrite=True)
        elif args.alg_type == "get_dist_to_border":
            # create new result file
            result_name = os.path.join(folder_save, "dist_border_" + code + ".csv")
            result_header = ['X', 'Y', 'DIST_BORDER_METERS', "NEAREST_COMM_ID"]
            if not args.append:
                with open(result_name, "w+", newline='') as file:
                    pass
                    filewriter = csv.writer(file, delimiter=",")
                    filewriter.writerow(result_header)
                    print(f"Created file {result_name}")
            with QGISContextManager():
                if args.load_data:
                    tiles_border = load_layer(os.path.join(folder_tiles, "border_" + code + "_" + crs_code + ".shp"))
                else:
                    tiles_border, _, err = get_border_of_country(code, tiles_path, save_flag=False,
                                                                 crs_name=crs_name,)
                # iterate over files with labeled pixels to get their x and y numbers
                for f in glob.glob(os.path.join(folder_labels, "pixel_label_" + code + "_0_0*")):
                    df = pd.read_csv(f, header=0)
                    print(f"Overall rows {len(df)}")
                    df = df[["X", "Y"]].drop_duplicates()
                    print(f"Unique pixels {len(df)}")
                    rows = []
                    k = 0
                    # iterate over retrieved pixels and get their distances to the selected tiles
                    for index, row in df.iterrows():
                        k += 1
                        if args.min_row < k < args.max_row:
                            i, j = row["X"], row["Y"]
                            rows.append(measure_dist(i, j, tiles_border,  # dist_type="point_to_tiles",
                                                     save_flag=False))
                            if k % 10000 == 1:
                                print(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())} k={k}")
                                with open(result_name, "a+", newline="") as file:
                                    filewriter = csv.writer(file)
                                    for row in rows:
                                        if row:
                                            filewriter.writerow(row)
                                    rows = []
                            if k % 100000 == 1:
                                #delete_layers()
                                t = time.time()
                                if args.load_data:
                                    tiles_border = load_layer(
                                        os.path.join(folder_tiles, "border_" + code + "_" + crs_code + ".shp"))
                                else:
                                    tiles_border, _, err = get_border_of_country(code, tiles_path, save_flag=False,
                                                                                 crs_name=crs_name, )
                                print(f"Loaded again {time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())} "
                                      f"in {(time.time()-t):.2} s k={k}")

                    if len(rows) > 0:
                        with open(result_name, "a+", newline="") as file:
                            filewriter = csv.writer(file)
                            for row in rows:
                                if row:
                                    filewriter.writerow(row)
        elif args.alg_type == "get_dist_to_lockdown":
            lockdowns_path = r"C:\Users\antonma\RA\lockdown\Response_measures.xlsx"
            last_day = datetime.datetime.strptime("07/01/2020", '%m/%d/%Y')
            df = pd.read_excel(lockdowns_path, sheet_name=None, header=0)
            print(df)
            # print(df.head())
            # affected	except
            df = df["Sheet1"]
            code = "DE"
            df = df[df["nuts_country"] == code]
            typ = []
            for index, row in df.iterrows():
                if row["date"] <= last_day:
                    nuts_yes = comm_no = []
                    if not is_nan(row["nuts_affected"], ):
                        nuts_yes = row["nuts_affected"].split()
                    if not is_nan(row["nuts_except"], ):
                        nuts_no = row["nuts_except"].split()
                    if not is_nan(row["comm_affected"], ):
                        comm_yes = row["comm_affected"].split()
                    if not is_nan(row["comm_except"], ):
                        comm_no = row["comm_except"].split()
                    print(row["date"], nuts_yes, nuts_no, comm_yes, comm_no)
            return 0
        print(f"{code} {time.time() - ts[-1]}")
        ts.append(time.time())


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
