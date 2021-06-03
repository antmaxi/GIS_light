import datetime
import glob
import math
import os
import traceback

import pandas as pd
from collections import defaultdict
import logging

import argparse

import config
import config as cfg

parser = argparse.ArgumentParser()
parser.add_argument('--lockdown_file', type=str, help='from which .csv take dates and nuts/comm_id',
                    default=os.path.join(".", "lockdown", "Response_measures.xlsx"))
parser.add_argument('--load_data', default=False, action='store_true',
                    help='whether to use loaded data or make it from scratch')

parser.add_argument('--debug', default=False, action='store_true', help='to run in debug (small) mode or not')
parser.add_argument('--export_layer', type=str, help='to save the created layer or not', default=False)
parser.add_argument('--save_name', type=str, help='name to save', default="point.sh")
parser.add_argument('--append', default=False, action='store_true',
                    help='whether to rewrite the result or append')

parser.add_argument('--code', type=str, help='which country code to process', default=None)
parser.add_argument('--alg_type', type=str, help='type of algorithm to run', default=None)
parser.add_argument('--alg_subtype', type=str, help='subtype of algorithm to run', default=None,
                    choices=("to_lockdown", "from_lockdown"))

parser.add_argument('--min_row', type=int, help='minimal index of row to process', default=0)
parser.add_argument('--max_row', type=int, help='maximal index of row to process', default=100000000)
parser.add_argument('--id', type=str, help='id for name of resulting files', default="")

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


def get_string_from_sorted_set_of_datetimes(times, format_string="%m-%d", sep="_"):
    """
        Create short coding for time interval from list, with the case when there are possibly several
    """
    start = times[0]
    date_string = start.strftime(format_string + sep)
    for i, t in enumerate(times):
        if (t - start).days != i:
            date_string += times[i - 1].strftime(format_string)
            start = times[i]
            date_string += "_" + start.strftime(format_string + sep)
    date_string += times[len(times) - 1].strftime(format_string)
    return date_string


def main(args):
    """
        1) Get border tiles or
        2) Get distance to the closest border
    """
    ##############################################################
    #                    INITIALIZATION
    ##############################################################
    folder_dist_save = cfg.folder_dist_save
    folder_tiles = cfg.folder_tiles
    folder_labels = cfg.folder_labels

    #  set logging
    if args.alg_type == "get_dist_lockdown":
        assert not (args.alg_subtype is None), "there should be subtype, 'to_lockdown' or 'from_lockdown'"
        log_string = "_".join([args.alg_type, args.alg_subtype, args.code])
    else:
        log_string = "_".join([args.alg_type, args.code])
    log_file = os.path.join(folder_dist_save, "_".join(["log", log_string + args.id + ".txt"]))
    if args.debug:
        logging.basicConfig(format='%(message)s',
                            level=logging.DEBUG)  # choose between WARNING - INFO - DEBUG
    else:
        logging.basicConfig(filename=log_file,
                            format='%(message)s',
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

    with open(log_file, 'w+') as f:
        pass
    # file with all countries' tiles
    path_to_tiles_layer = os.path.join(folder_tiles, tiles_filename)

    assert not args.alg_type is None
    # if args.alg_type is None:
    # args.alg_type = "extract_border"
    # args.alg_type = "get_dist_to_border"
    # args.alg_type = "get_dist_lockdown"
    if args.code is not None:
        codes = [args.code, ]
    else:
        codes = [
            # "AT",
            # "BE",
            # "CH", "CZ", "DK",
            # "IE",
            # "NL",
            # "PL", "PT",
            # "LI", "MC", "SM",
            # "AD",
            # "DE",
            # "FR", "ES",  "IT", "GB", "ND"
        ]
    for code in codes:
        # code is prefix of COMM_ID for tiles
        print(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())} Started {code}")
        if args.alg_type == "extract_border":
            save_path = os.path.join(folder_tiles, "border_" + code + "_" + crs_code + ".shp")
            with QGISContextManager():
                tiles_to_measure_dist_to, _, err = get_border_of_country(code, tiles_path, save_flag=True,
                                                                         save_path=save_path,
                                                                         crs_name=crs_name, rewrite=True)
        elif args.alg_type == "get_dist_to_border" or args.alg_type == "get_dist_lockdown":
            with QGISContextManager():
                '''
                    get result name, header and tiles to measure distance to 
                '''
                if args.alg_type == "get_dist_to_border":
                    result_name = os.path.join(folder_dist_save, "dist_to_border_" + code + args.id + ".csv")
                    result_header = ['X', 'Y', "COUNTRY_CODE", "NUTS_CODE", "COMM_ID",
                                     'DIST_BORDER_KM', "NEAREST_COMM_ID", "NEAREST_NUTS", "NEAREST_COUNTRY_CODE"]
                    # get tiles
                    if args.load_data:
                        tiles_to_measure_dist_to = load_layer(
                            os.path.join(folder_tiles, "border_" + code + "_" + crs_code + ".shp"))
                    else:
                        tiles_to_measure_dist_to, _, err = get_border_of_country(code, tiles_path, save_flag=False,
                                                                                 crs_name=crs_name,
                                                                                 used_field="nuts")
                    expr_to_dates = {None: [None] * 5}  # stub
                elif args.alg_type == "get_dist_lockdown":
                    last_day = datetime.datetime.strptime("10/15/2020", '%m/%d/%Y')
                    result_header = config.dist_header
                    # get tiles
                    lockdowns_path = args.lockdown_file
                    # last day to check lockdowns

                    df = pd.read_excel(lockdowns_path, sheet_name=None, header=0)
                    df = df["Sheet1"]
                    code_xlsx = code if code not in ("GB", "ND") else "UK"
                    df = df[df["nuts_country"] == code_xlsx]
                    expr_to_dates = dict()
                    for index, row in df.iterrows():
                        if row["date"] <= last_day:
                            nuts_yes = nuts_no = comm_yes = comm_no = []
                            # choose if measure distance to or from local lockdown
                            if args.alg_subtype == "to_lockdown":
                                yes_suffix = "affected"
                                no_suffix = "except"
                            elif args.alg_subtype == "from_lockdown":
                                yes_suffix = "except"
                                no_suffix = "affected"
                            else:
                                raise NotImplementedError(f"Not known subtype {args.alg_subtype}")
                            if not is_nan(row["nuts_" + no_suffix], ):
                                nuts_no = row["nuts_" + no_suffix].split()
                            if not is_nan(row["nuts_" + yes_suffix], ):
                                nuts_yes = row["nuts_" + yes_suffix].split()
                            if not is_nan(row["comm_" + no_suffix], ):
                                comm_no = row["comm_" + no_suffix].split()
                            if not is_nan(row["comm_" + yes_suffix], ):
                                comm_yes = row["comm_" + yes_suffix].split()

                            for codes in (comm_yes, comm_no, nuts_yes, nuts_no):
                                if code in codes:
                                    codes.remove(code)
                            # if the whole country in lockdown
                            if comm_yes == [] and nuts_yes == [] and comm_no == [] and nuts_no == []:
                                continue
                            if (code in comm_no or code in nuts_no) and comm_yes == [] and nuts_yes == []:
                                continue
                            if (code in comm_yes or code in nuts_yes) and comm_no == [] and nuts_no == []:
                                continue
                            expr_curr = expression_from_nuts_comm(nuts_yes=nuts_yes, nuts_no=nuts_no,
                                                                  comm_yes=comm_yes, comm_no=comm_no)
                            if not expr_curr:
                                continue

                            expr_curr = "\"NUTS_CODE\" LIKE '" + code_xlsx + "%' AND (" + expr_curr + ")"  # or COMM_ID
                            if expr_curr in expr_to_dates:
                                expr_to_dates[expr_curr][0].append(row["date"])
                            else:
                                expr_to_dates[expr_curr] = [[row["date"], ], ]
                            if len(expr_to_dates[expr_curr]) == 1:
                                for codes in (comm_yes, comm_no, nuts_yes, nuts_no):
                                    expr_to_dates[expr_curr].append(codes)
                curr = 0
                for expr, data in expr_to_dates.items():
                    times, comm_yes, comm_no, nuts_yes, nuts_no = data
                    curr += 1
                    if args.alg_type == "get_dist_to_border":
                        result_name = os.path.join(folder_dist_save, "dist_border_" + code + args.id + ".csv")
                    elif args.alg_type == "get_dist_lockdown":
                        # TODO optimize - get only the border of non-lockdown instead of the whole lockdown area
                        if not expr:
                            continue
                        date_string = get_string_from_sorted_set_of_datetimes(times, format_string="%m-%d", sep="--")
                        print(date_string)
                        logger.info(expr)
                        if args.alg_subtype is not None:
                            result_name = os.path.join(folder_dist_save,
                                                       "dist_" + args.alg_subtype + "_"
                                                       + code + "_" + date_string + "_" + args.id + ".csv")
                        else:
                            raise NotImplementedError("Need subtype for lockdown measuring")
                        tiles_to_measure_dist_to, _, _ = layer_filter_from_expression(tiles_path, expr=expr,
                                                                                      save_flag=False,
                                                                                      save_name="saved13.shp"
                                                                                      )
                    # create new result file
                    if not args.append:
                        with open(result_name, "w+", newline='') as file:
                            pass
                            filewriter = csv.writer(file, delimiter=",")
                            filewriter.writerow(result_header)
                            print(f"Created file {result_name}")

                    '''
                        iterate over files with labeled pixels to get their x and y numbers
                        assume that in different files (if so) no pixels are repeated
                    '''
                    for f in glob.glob(os.path.join(folder_labels, "pixel_label_" + code + "_0_0*")):
                        df = pd.read_csv(f, header=0)
                        print(f"Overall rows {len(df)}")
                        df = df[["X", "Y", "NUTS_CODE", "COMM_ID"]].drop_duplicates()
                        print(f"Unique pixels {len(df)}")
                        rows = []
                        k = 0
                        print(f"nuts_yes={nuts_yes}, nuts_no={nuts_no}, comm_yes={comm_yes}, comm_no{comm_no}")
                        # iterate over retrieved pixels and get their distances to the selected tiles
                        count = 0
                        for index, row in df.iterrows():  # TODO add some parallelization
                            k += 1
                            if args.min_row < k <= args.max_row:
                                i, j = row["X"], row["Y"]
                                # check that pixel is not from tiles to measure dist to
                                if args.alg_type == "get_dist_lockdown":

                                    try:
                                        if args.alg_subtype == "to_lockdown":
                                            f = True
                                            for nuts in nuts_yes:
                                                if str(row["NUTS_CODE"]).startswith(nuts):
                                                    #print(str(row["NUTS_CODE"]))
                                                    f = False
                                            for comm in comm_yes:
                                                if str(row["COMM_ID"]).startswith(comm):
                                                    #print(str(row["NUTS_CODE"]))
                                                    f = False
                                            for nuts in nuts_no:
                                                if str(row["NUTS_CODE"]).startswith(nuts):
                                                    f = True
                                            for comm in comm_no:
                                                if str(row["COMM_ID"]).startswith(comm):
                                                    f = True
                                        else:
                                            f = False
                                            for nuts in nuts_no:
                                                if str(row["NUTS_CODE"]).startswith(nuts):
                                                    f = True
                                            for comm in comm_no:
                                                if str(row["COMM_ID"]).startswith(comm):
                                                    f = True
                                            for nuts in nuts_yes:
                                                if str(row["NUTS_CODE"]).startswith(nuts):
                                                    f = False
                                            for comm in comm_yes:
                                                if str(row["COMM_ID"]).startswith(comm):
                                                    f = False
                                    except Exception as e:
                                        logging.error(traceback.format_exc())
                                    if f:
                                        pass
                                    else:
                                        #print(row["NUTS_CODE"])
                                        continue
                                count += 1
                                rows.append([i, j, code, row["NUTS_CODE"], row["COMM_ID"]]  # current data
                                            + measure_dist(i, j, tiles_to_measure_dist_to,  # nearest tile data
                                                           )
                                            )
                                # write rows to the file in chunks
                                if rows and k % 10000 == 1:
                                    print(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())} k={k} "
                                          f"written={count}")
                                    with open(result_name, "a+", newline="") as file:
                                        filewriter = csv.writer(file)
                                        for r in rows:
                                            if r:
                                                filewriter.writerow(r)
                                        rows = []
                        if len(rows) > 0:
                            print(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())} k={k} "
                                  f"written={count}")
                            with open(result_name, "a+", newline="") as file:
                                filewriter = csv.writer(file)
                                for row in rows:
                                    if row:
                                        filewriter.writerow(row)
        else:
            raise NotImplementedError(f"Not known alg type {args.alg_type}")

        print(f"{code} {time.time() - ts[-1]}")
        ts.append(time.time())


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
