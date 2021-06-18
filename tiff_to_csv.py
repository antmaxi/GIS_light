import glob
import time

from osgeo import gdal
import pandas as pd
import numpy as np
import os
import sys

from pathlib import Path
import re

import shutil
import argparse
import subprocess
import concurrent.futures

import traceback
import logging

logging.basicConfig(format='%(asctime)s | %(levelname)s | %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger("conversion")
logger.setLevel(getattr(logging, "DEBUG"))
# TODO add logger writing to log file

parser = argparse.ArgumentParser()
parser.add_argument('--date_min', type=str, help='start date of files to process (included)', default="20180101")
parser.add_argument('--date_max', type=str, help='end date of files to process (included)',
                    default="20211231")
parser.add_argument('--type', type=str, help='folder to use', choices=("rade9d", "cloud_cover",
                                                                       "vcmcfg", "vcmslcfg"),
                    required=True)
parser.add_argument('--extension', default=".parquet", type=str, choices=(".parquet", ".csv"),
                    help='extension of result', )
parser.add_argument('--workers', type=int, help='how many processes/threads to use',
                    default="4")
parser.add_argument('--parall_type', type=str, choices=("process", "thread"), help='use processes/threads',
                    default="thread")
parser.add_argument("--parquet_to_csv", default=False, action='store_true',
                    help='whether to convert from parquet to csv after conversion from tif to parquet')
parser.add_argument("--delete", default=False, action='store_true',
                    help='whether to delete produced and uploaded to the server files')
parser.add_argument("--from_tgz", default=False, action='store_true',
                    help='whether to decompress files from .tgz')


def get_first_number_from_string(st):
    return re.findall(r'\d+', st)[0]


def convert_parquet_to_csv(path_parquet, args):
    """ path_parquet - path to the .parquet file on local machine"""
    filename = Path(path_parquet).stem
    logger.info(f"Processing {filename}")
    date = get_first_number_from_string(filename)
    local_log_file = os.path.join(folder_logs, date + args.extension + ".log")

    dir = os.path.dirname(path_parquet)
    path_csv = os.path.join(dir, filename + ".csv")

    with open(local_log_file, 'w+'):
        pass
    try:
        df = pd.read_parquet(path_parquet)
        logger.debug(f"Read parquet from {path_parquet}")

        def fn(row):
            # print(row.X, row.Y, row.X + (row.Y - y1) * dx - x1)
            return row.X + (row.Y - y1) * dx - x1

        # get single ID instead of X and Y
        # df.apply(lambda row: row.X + (row.Y - y1) * dx - x1, axis=1)  # size of raster is 86401 x 33601
        df['ID'] = df["X"] - x1 + (df["Y"] - y1)
        df = df.drop(columns=['X', 'Y'])  # int32 max 2,147,483,647
        # round float values to get lesser size
        # in rade9d step of value is 0.1, therefore 1 decimal is enough
        # logger.debug(f"Before decimal rounding have:\n {df.head(10)}")
        if args.type in ("rade9d",):
            df["VALUE"] = df["VALUE"].round(1)
            logger.debug(f"Round to 1 decimal")
        elif args.type.startswith("vcm"):
            # in monthly data ending by "avg_rade9h" step of value is 0.01, therefore 2 decimals are enough
            if filename.endswith("avg_rade9h"):
                df["VALUE"] = df["VALUE"].round(2)
                logger.debug(f"Round to 2 decimals")
            elif filename.endswith("cf_cvg"):
                pass
            elif filename.endswith("cvg"):
                pass
            else:
                raise ValueError("not known type of filename")
        elif args.type in ("cloud_cover",):
            pass
        else:
            raise ValueError("Not known type of processing")
        df = df[["ID", "VALUE"]]
        df.to_csv(path_csv, index=False)
        logger.debug(f"Dumped parquet to {path_csv}")
        path_device_csv = os.path.join(folder_device_processed_csv, args.type, filename + ".csv")
        logger.debug(f"Started uploading of {path_csv}")
        shutil.copyfile(path_csv, path_device_csv)
        logger.debug(f"Uploaded  {path_csv} to {path_device_csv}")
        if args.delete:
            if (sys.getsizeof(path_csv) == sys.getsizeof(path_device_csv)):
                if os.path.exists(path_csv):
                    os.remove(path_csv)
                    logger.debug(f"Deleted csv {path_csv}")
                if os.path.exists(path_parquet):
                    os.remove(path_parquet)
                    logger.debug(f"Deleted parquet {path_parquet}")
        # os.remove(path_parquet)
        return get_first_number_from_string(filename)
    except Exception as exc:
        with open(local_log_file, 'a+') as f:
            f.write(f"{date} {exc}\n")
        print("EXCEPTION")
        traceback.print_exc()
        if os.path.exists(path_csv):
            os.remove(path_csv)
            logger.debug(f"Deleted csv {path_csv}")
        return None


def convert_geotiff_to_parquet(file_path, args):
    """ file_path - path to the .tif file on remote device """
    filename = Path(file_path).stem
    date = get_first_number_from_string(filename)
    if not (args.date_min <= date <= args.date_max):
        return None
    local_log_file = os.path.join(folder_logs, date + ".log")
    with open(local_log_file, 'w+'):
        pass
    path_raster_cropped = os.path.join(folder_temp, filename + "_EUcropped.xyz")
    path_csv = os.path.join(folder_result, filename + ".csv")
    path_parquet = os.path.join(folder_result, filename + ".parquet")
    logger.debug(
        f"Processing {filename} from {folder_to_process}\n "
        f"save results to {os.path.join(folder_result, filename) + '.parquet and/or .csv'}")

    try:
        # download raster to process
        path_raster = os.path.join(folder_to_process, filename + ".tif")
        if args.type in ("cloud_cover", "rade9d"):
            if not os.path.exists(path_raster) or (sys.getsizeof(path_raster) != sys.getsizeof(file_path)):
                shutil.copyfile(file_path, path_raster)
                logger.debug(f"Downloaded {file_path} to {path_raster}")
            else:
                logger.debug(f"File {path_raster} is of the right size, no need to download again")

        # get one area - approx Western Europe. Type e.g. XYZ or GTIFF
        com_string = "gdal_translate -of XYZ -q -srcwin " + str(x1) + ", " + str(y1) + ", " + str(
            dx) + ", " + str(dy) + " " + str(path_raster) + " " + str(path_raster_cropped)
        subprocess.run(com_string)
        df = pd.read_csv(path_raster_cropped, sep=" ", header=None, names=["X", "Y", "VALUE"])
        logger.debug(f"Cropped from {path_raster} to {path_raster_cropped}")
        # get metadata of raster
        ds = gdal.Open(path_raster)  # width = ds.RasterXSize #height = ds.RasterYSize
        # convert coords to pixel ids
        gt = ds.GetGeoTransform()  # (-180.00208333335, 0.0041666667, 0.0, 75.00208333335, 0.0, -0.0041666667)
        del ds
        minX = gt[0]  # minY = gt[3] + width * gt[4] + height * gt[5]
        maxY = gt[3]  # maxX = gt[0] + width * gt[1] + height * gt[2]
        pixel_size = gt[1]  # width, [5] - height
        df.iloc[:, 0] = ((df.iloc[:, 0] - minX) / pixel_size).astype("int32")
        df.iloc[:, 1] = ((maxY - df.iloc[:, 1]) / pixel_size).astype("int32")
        # print(df)
        # print(df.iloc[:, 2].value_counts())
        # print(width, height, minX, minY, maxX, maxY)
        assert df.shape[0] == dx * dy, f"size {df.shape[0]} instead of {dx * dy}"

        df.to_parquet(path_parquet, index=False)
        logger.debug(f"Size of resulting file {path_parquet} is "
                     f"{os.path.getsize(path_parquet) / pow(2, 20):.1f} MB")
        # delete used files
        if os.path.exists(path_raster_cropped):
            os.remove(path_raster_cropped)
            logger.debug(f"Deleted cropped {path_raster_cropped}")
        if args.delete:
            if os.path.exists(path_raster):
                os.remove(path_raster)
                logger.debug(f"Deleted raster {path_raster}")
        # upload result to device
        path_result_device = os.path.join(folder_device_processed_parquet, args.type, filename + ".parquet")
        logger.debug(f"Started uploading of {path_parquet}")
        shutil.copyfile(path_parquet, path_result_device)
        logger.debug(f"Uploaded {path_parquet} to {path_result_device}")
        # TODO update list of files - account for python newly updated ones
    except (KeyboardInterrupt, SystemExit):
        if os.path.exists(path_parquet):
            pass
            # os.remove(path_parquet)
            # logger.debug(f"Deleted {path_parquet}")
    except Exception as exc:
        with open(local_log_file, 'a+') as f:
            f.write(f"{date} {exc}\n")
        print("EXCEPTION")
        traceback.print_exc()
        if os.path.exists(path_parquet):
            os.remove(path_parquet)
            logger.debug(f"Deleted parquet {path_parquet}")
        return None

    try:
        if args.parquet_to_csv:
            convert_parquet_to_csv(path_parquet, args)
            if args.delete and os.path.exists(path_parquet):
                os.remove(path_parquet)
                logger.debug(f"Deleted parquet {path_parquet}")
    except Exception as exc:
        with open(local_log_file, 'a+') as f:
            f.write(f"{date} {exc}\n")
        print("EXCEPTION")
        traceback.print_exc()
        if os.path.exists(path_csv):
            os.remove(path_csv)
            logger.debug(f"Deleted csv {path_csv}")
        return None
    return date


def main(args):
    """
        args.extension == ".csv" - take parquet from computer, convert to csv and upload to the drive
        args.extension == ".parquet" - download .tif from drive, crop and convert to parquet and upload to the drive
    """

    while True:
        print(time.strftime(time_format))
        dates = []

        if args.parall_type == "process":
            parallel_executor = concurrent.futures.ProcessPoolExecutor(max_workers=args.workers)
        elif args.parall_type == "thread":
            parallel_executor = concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)
        else:
            raise NotImplementedError

        made_parquets = [Path(f).stem
                         for f in list(glob.glob(os.path.join(folder_result, "*" + ".parquet")))] \
                        + [Path(f).stem
                           for f in
                           list(glob.glob(os.path.join(folder_device_processed_parquet_current, "*.parquet")))]
        made_csvs = [Path(f).stem
                     for f in
                     list(glob.glob(os.path.join(folder_device_processed_csv_current, "*.csv")))]

        filepaths = []
        filepaths_tif = []
        filepaths_parquet = []
        # TODO better system
        # .parquet to make from tif parquets, then flag --parquet_to_csv does also to csv conversion after
        # get filepaths to process
        if args.extension == ".parquet":
            if args.from_tgz:
                pass
            if args.parquet_to_csv:
                for filepath_curr in sorted(glob.glob(os.path.join(folder_result, "*.parquet"))):
                    filename_curr = Path(filepath_curr).stem
                    if filename_curr not in made_csvs:
                        filepaths_parquet.append(filepath_curr)
            # first do parquet to csv if can, then tif - parquet - csv
            if filepaths_parquet:
                func = convert_parquet_to_csv
                filepaths = filepaths_parquet
                logger.debug(f"Process parquet_to_csv")
            else:
                for filepath_curr in sorted(glob.glob(os.path.join(folder_device_raw_current, "*.tif"))):
                    filename_curr = Path(filepath_curr).stem
                    if filename_curr not in made_parquets:
                        filepaths_tif.append(filepath_curr)
                func = convert_geotiff_to_parquet
                filepaths = filepaths_tif
                logger.debug(f"Process geotiff_to_parquet")
            # filepaths = filepaths[0:1]  # TODO delete not in debug

        # only existing parquets to csv
        elif args.extension == ".csv":
            # + [Path(f).stem
            #   for f in
            #   list(glob.glob(os.path.join(folder_local_data, args.type, "result", "*" + ".csv")))]

            for filepath_curr in sorted(glob.glob(os.path.join(folder_result, "*.parquet"))):
                filename_curr = Path(filepath_curr).stem
                if filename_curr not in made_csvs:
                    filepaths_parquet.append(filepath_curr)
            func = convert_parquet_to_csv
            filepaths = filepaths_parquet
            logger.debug(f"Process parquet_to_csv")
        if not filepaths:
            break

        def get_first_number_in_filename_from_path(path):
            get_first_number_from_string(Path(path).stem)

        logger.debug(" ".join(map(get_first_number_in_filename_from_path, filepaths)))
        # dates = ([re.findall(r'\d+', Path(f).stem)[0] for f in files])
        with parallel_executor as executor:

            future_to_url = {executor.submit(func, filepath, args):
                                 filepath for filepath in filepaths}

            for future in concurrent.futures.as_completed(future_to_url):
                f = future_to_url[future]
                try:
                    date = future.result()
                    dates.append(date)
                    logger.info(date)
                except Exception as exc:
                    print('%r generated an exception: %s' % (f, exc))
                else:
                    print(f'{f} processed with result {date}')

        logger.info(f"Done dates {dates}")
        if len(dates):
            logger.info(f"from {dates[0]} to {dates[-1]}")
        # time.sleep(30 * 60)


if __name__ == '__main__':
    args = parser.parse_args()
    # existing folders
    folder_local_data = r"C:\Users\antonma\RA\data"
    folder_to_process = os.path.join(folder_local_data, args.type)
    # possibly new folders
    folder_result = os.path.join(folder_local_data, args.type, "result")
    folder_temp = os.path.join(folder_local_data, "temp")
    folder_logs = os.path.join(folder_local_data, args.type, "logs")

    # remote device - existing folders
    folder_device_raw = r"Z:\Projekte\COVID19_Remote\01_raw_data"
    # set general type, to find folder with raw .tif data on the remote device
    if args.type in ("cloud_cover", "rade9d"):
        t = "nightly"
    else:
        t = "monthly"
    # remote device - possibly new folders
    folder_device_raw_current = os.path.join(folder_device_raw, t, args.type)
    folder_device_processed = r"Z:\Projekte\COVID19_Remote\02_processed_data\raster"
    folder_device_processed_parquet = os.path.join(folder_device_processed, "parquet")
    folder_device_processed_csv = os.path.join(folder_device_processed, "csv")
    folder_device_processed_parquet_current = os.path.join(folder_device_processed_parquet, args.type)
    folder_device_processed_csv_current = os.path.join(folder_device_processed_csv, args.type)
    for folder in (folder_temp, folder_logs, folder_result,
                   folder_device_processed,
                   folder_device_processed_csv, folder_device_processed_parquet,
                   folder_device_processed_parquet_current, folder_device_processed_csv_current,
                   folder_device_raw_current):
        os.makedirs(folder, exist_ok=True)

    global_log_file = os.path.join(folder_logs, args.type + time.strftime("_%Y%m%d_%H%M%S") + ".log")

    fh = logging.FileHandler(global_log_file)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

    time_format = "%d%m %H:%M:%S"
    # TIFF to CSV
    # xyz = gdal.Translate("dem2.xyz", ds)
    x1_0 = 40272
    x2_0 = 49921
    y1_0 = 2808
    y2_0 = 10441
    if args.type.startswith("vcm"):  # monthly
        # -60.0020833333500008,0.0020827333499938 : 59.9979176266500076,75.0020833333500008
        # 28800 x 18000 0.004166666700000000098,-0.004166666700000000098
        # 240 pix /1 degree

        # account for this shift of the presented in .tif area:
        x1 = x1_0 - 240 * 120  # corner of zero: -180 -> -60
        x2 = x2_0 - 240 * 120
        y1 = y1_0  # corner of zero: 75 -> 75
        y2 = y2_0
    else:  # the whole world for cloud_cover, rade9d
        # -180.0020833333499866, -65.0020844533500082: 180.0020862133499975, 75.0020833333500008
        # 86401 x 33601 0.004166666700000000098,-0.004166666700000000098
        x1 = x1_0
        x2 = x2_0
        y1 = y1_0
        y2 = y2_0
    output_bounds = [-12.2, 31.5, 28, 63.3]
    dx = x2 - x1 + 1  # 9650
    dy = y2 - y1 + 1  # 7634
    # dx * dy = total pixels = 73668100

    main(args)
