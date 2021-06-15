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
parser.add_argument('--type', type=str, help='folder to use', choices=("rade9d", "cloud_cover"),
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


def get_first_number_from_string(st):
    return re.findall(r'\d+', st)[0]


def convert_parquet_to_csv(path_parquet, args):
    """ path_parquet - path to the .parquet file on local machine"""
    filename = Path(path_parquet).stem
    print(filename)
    date = get_first_number_from_string(filename)
    log_file = os.path.join(folder_logs, date + args.extension + ".log")

    dir = os.path.dirname(path_parquet)
    path_csv = os.path.join(dir, filename + ".csv")

    with open(log_file, 'w+'):
        pass
    try:
        df = pd.read_parquet(path_parquet)
        logger.debug(f"Read parquet from {path_parquet}")
        def fn(row):
            #print(row.X, row.Y, row.X + (row.Y - y1) * dx - x1)
            return row.X + (row.Y - y1) * dx - x1
        df['ID'] = df["X"] - x1 + (df["Y"] - y1)#df.apply(lambda row: row.X + (row.Y - y1) * dx - x1, axis=1)  # size of raster is 86401 x 33601
        df = df.drop(columns=['X', 'Y'])  # int32 max 2,147,483,647
        df = df[["ID", "VALUE"]]
        df.to_csv(path_csv, index=False)
        logger.debug(f"Dumped parquet to {path_csv}")
        path_device_csv = os.path.join(folder_device_processed_csv, args.type, filename + ".csv")
        shutil.copyfile(path_csv, path_device_csv)
        if args.delete:
            os.remove(path_csv)
            os.remove(path_parquet)
        #os.remove(path_parquet)
        logger.debug(f"Done uploading of {path_csv} to {path_device_csv}")
        return get_first_number_from_string(filename)
    except Exception as exc:
        with open(log_file, 'a+') as f:
            f.write(f"{date} {exc}\n")
        print("EXCEPTION")
        traceback.print_exc()
        if os.path.exists(path_csv):
            os.remove(path_csv)
        return None


def convert_geotiff_to_parquet(file_path, args):
    """ file_path - path to the .tif file on remote device """
    filename = Path(file_path).stem
    date = get_first_number_from_string(filename)
    if not (args.date_min <= date <= args.date_max):
        return None
    log_file = os.path.join(folder_logs, date + ".log")
    with open(log_file, 'w+'):
        pass
    path_raster_cropped = os.path.join(folder_temp, filename + "_EUcropped.xyz")
    path_csv = os.path.join(folder_result, filename + ".csv")
    path_parquet = os.path.join(folder_result, filename + ".parquet")
    logger.debug(f"Processing {filename} ",
          f"save results to {os.path.join(folder_result, filename) + '.parquet and/or .csv'}")

    try:
        # download raster to process
        path_raster = os.path.join(folder_to_process, filename + ".tif")
        if 1:
            if not os.path.exists(path_raster) or (sys.getsizeof(path_raster) != sys.getsizeof(file_path)):
                shutil.copyfile(file_path, path_raster)
                logger.debug(f"Downloaded {file_path} to {path_raster}")
            else:
                logger.debug(f"File {path_raster} is of the right size, no need to download again")

            # get one area - approx Western Europe
            com_string = "gdal_translate -of XYZ -q -srcwin " + str(x1) + ", " + str(y1) + ", " + str(
                dx) + ", " + str(dy) + " " + str(path_raster) + " " + str(path_raster_cropped)
            subprocess.run(com_string)
            df = pd.read_csv(path_raster_cropped, sep=" ", header=None, names=["X", "Y", "VALUE"])
            logger.debug(f"Cropped from {path_raster} to {path_raster_cropped}")
        # get metadata of raster
        ds = gdal.Open(path_raster)
        width = ds.RasterXSize
        height = ds.RasterYSize
        # convert coords to pixel ids
        gt = ds.GetGeoTransform() #(-180.00208333335, 0.0041666667, 0.0, 75.00208333335, 0.0, -0.0041666667)
        del ds
        minX = gt[0]
        #minY = gt[3] + width * gt[4] + height * gt[5]
        # maxX = gt[0] + width * gt[1] + height * gt[2]
        maxY = gt[3]
        pixel_size = gt[1]  # width, [5] - height
        df.iloc[:, 0] = ((df.iloc[:, 0] - minX) / pixel_size).astype("int32")
        df.iloc[:, 1] = ((maxY - df.iloc[:, 1]) / pixel_size).astype("int32")
        # print(df)
        # print(df.iloc[:, 2].value_counts())
        # print(width, height, minX, minY, maxX, maxY)
        assert df.shape[0] == dx * dy,  f"size {df.shape[0]} instead of {dx * dy}"

        df.to_parquet(path_parquet, index=False)
        # ext = ".parquet"
        # parquet_to_csv(path_parquet, path_csv)
        # df.to_csv(path_csv, index=False)
        # print(time.strftime(time_format) + " Done saving to csv")
        logger.debug(f"Size of resulting file {path_parquet} is "
              f"{os.path.getsize(path_parquet) / pow(2, 20):.1f} MB")
        # delete used files
        if args.delete:
            if os.path.exists(path_raster_cropped):
                os.remove(path_raster_cropped)
            if os.path.exists(path_raster):
                os.remove(path_raster)
        # upload result to device
        path_result_device = os.path.join(folder_device_processed_parquet, args.type, filename + ".parquet")
        shutil.copyfile(path_parquet, path_result_device)
        logger.debug(f"Uploaded {path_parquet} to {path_result_device}")

        # TODO move result to folder_final/... (sudo needed?)
        # update list of files - account for python newly updated ones
        # files = glob.glob(os.path.join(folder_to_process, "*.tif"))
    except (KeyboardInterrupt, SystemExit):
        if os.path.exists(path_parquet):
            os.remove(path_parquet)
    except Exception as exc:
        with open(log_file, 'a+') as f:
            f.write(f"{date} {exc}\n")
        print("EXCEPTION")
        traceback.print_exc()
        if os.path.exists(path_parquet):
            os.remove(path_parquet)
        return None

    try:
        if args.parquet_to_csv:
            convert_parquet_to_csv(path_parquet, args)
            if args.delete:
                os.remove(path_parquet)
    except Exception as exc:
        with open(log_file, 'a+') as f:
            f.write(f"{date} {exc}\n")
        print("EXCEPTION")
        traceback.print_exc()
        if os.path.exists(path_csv):
            os.remove(path_csv)
        return None
    return date


def main(args):
    """
        args.extension == ".csv" - take parquet from computer, convert to csv and upload to the drive
        args.extension == ".parquet" - download .tif from drive, crop and convert to parquet and upload to the drive
    """
    os.makedirs(folder_temp, exist_ok=True)
    os.makedirs(folder_logs, exist_ok=True)
    os.makedirs(folder_result, exist_ok=True)

    while True:
        print(time.strftime(time_format))
        dates = []
        t = "nightly"
        ext = args.extension

        # get filepaths to process
        if args.extension == ".parquet":
            made_filenames = [Path(f).stem
                              for f in list(glob.glob(os.path.join(folder_local_data, args.type, "result", "*"
                                                                   + ".parquet")))] \
                             + [Path(f).stem
                                for f in
                                list(glob.glob(os.path.join(folder_device_processed_parquet, args.type, "*"
                                                            + ".parquet")))]
            filepaths = []
            for filepath_curr in sorted(glob.glob(os.path.join(folder_device_raw, t, args.type, "*.tif"))):
                filename_curr = Path(filepath_curr).stem
                if filename_curr not in made_filenames:
                    filepaths.append(filepath_curr)

            func = convert_geotiff_to_parquet
        elif args.extension == ".csv":
            made_filenames = [Path(f).stem
                              for f in list(glob.glob(os.path.join(folder_device_processed_csv, args.type, "*.csv")))]# \
                             #+ [Path(f).stem
                             #   for f in
                             #   list(glob.glob(os.path.join(folder_local_data, args.type, "result", "*" + ".csv")))]

            filepaths = []
            for filepath_curr in sorted(glob.glob(os.path.join(folder_result, "*.parquet"))):
                filename_curr = Path(filepath_curr).stem
                if filename_curr not in made_filenames:
                    filepaths.append(filepath_curr)
            func = convert_parquet_to_csv

        # dates = ([re.findall(r'\d+', Path(f).stem)[0] for f in files])
        if args.parall_type == "process":
            ParallelExecutor = concurrent.futures.ProcessPoolExecutor(max_workers=args.workers)
        elif args.parall_type == "thread":
            ParallelExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)
        else:
            raise NotImplementedError

        with ParallelExecutor as executor:

            future_to_url = {executor.submit(func, f, args):
                                 f for f in filepaths}

            for future in concurrent.futures.as_completed(future_to_url):
                f = future_to_url[future]
                try:
                    date = future.result()
                    dates.append(date)
                    print(date)
                except Exception as exc:
                    print('%r generated an exception: %s' % (f, exc))
                else:
                    print(f'{f} processed with result {date}')

        logger.info(f"Done dates {dates}")
        logger.info(f"from {dates[0]} to {dates[-1]}")
        # time.sleep(30 * 60)


if __name__ == '__main__':
    args = parser.parse_args()

    folder_data = r"C:\Users\antonma\RA\data"
    folder_to_process = os.path.join(folder_data, args.type)
    folder_result = os.path.join(folder_data, args.type, "result")
    folder_temp = os.path.join(folder_data, "temp")
    folder_logs = os.path.join(folder_data, args.type, "logs")

    folder_device_raw = r"Z:\Projekte\COVID19_Remote\01_raw_data"
    folder_device_processed = r"Z:\Projekte\COVID19_Remote\02_processed_data\raster"
    folder_device_processed_parquet = os.path.join(folder_device_processed, "parquet")
    folder_device_processed_csv = os.path.join(folder_device_processed, "csv")

    folder_local_data = r"C:\Users\antonma\RA\data"

    folder_final = os.path.join(r"Z:\Projekte\COVID19_Remote\02_processed_data\raster", "parquet",
                                args.type)

    time_format = "%d%m %H:%M:%S"
    # TIFF to CSV
    # xyz = gdal.Translate("dem2.xyz", ds)4
    x1 = 40272
    x2 = 49921
    y1 = 2808
    y2 = 10441
    output_bounds = [-12.2, 31.5, 28, 63.3]
    dx = x2 - x1 + 1
    dy = y2 - y1 + 1

    main(args)
