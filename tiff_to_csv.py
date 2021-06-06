import glob
import time

from osgeo import gdal
import pandas as pd
import numpy as np
import os
from pathlib import Path
import re

import shutil
import argparse
import subprocess
import concurrent.futures

import traceback

parser = argparse.ArgumentParser()
parser.add_argument('--date_min', type=str, help='start date of files to process (included)', default="20180101")
parser.add_argument('--date_max', type=str, help='end date of files to process (included)',
                    default="20211231")
parser.add_argument('--type', type=str, help='folder to use', choices=("rade9d", "cloud_cover"),
                    required=True)
parser.add_argument('--workers', type=int, help='how many processes/threads to use',
                    default="4")
parser.add_argument('--parall_type', type=str, choices=("process", "thread"), help='use processes/threads',
                    default="thread")


def parquet_to_csv(path_parquet, path_csv):
    df = pd.read_parquet(path_parquet)
    df.to_csv(path_csv, index=False)


def convert_geotiff_to_csv_parquet(f):
    filename = Path(f).stem
    date = re.findall(r'\d+', filename)[0]
    if not (args.date_min <= date <= args.date_max):
        return None
    log_file = os.path.join(folder_logs, date + ".log")
    with open(log_file, 'w+'):
        pass
    path_raster = os.path.join(folder_to_process, filename + ".tif")
    path_raster_cropped = os.path.join(folder_temp, filename + "_EUcropped.xyz")
    path_csv = os.path.join(folder_result, filename + ".csv")
    path_parquet = os.path.join(folder_result, filename + ".parquet")
    print(f"Processing {filename} ",
          f"save results to {os.path.join(folder_result, filename) + ', .parquet and/or .csv'}")

    try:
        # get one area - approx Western Europe
        com_string = "gdal_translate -of XYZ -q -srcwin " + str(x1) + ", " + str(y1) + ", " + str(
            dx) + ", " + str(dy) + " " + str(path_raster) + " " + str(path_raster_cropped)
        subprocess.run(com_string)
        # print(time.strftime(time_format) + " Done gdal_translate")

        df = pd.read_csv(path_raster_cropped, sep=" ", header=None, names=["X", "Y", "VALUE"])
        # print(df.size)
        # print(time.strftime(time_format) + " Read xyz")
        ds = gdal.Open(path_raster)
        width = ds.RasterXSize
        height = ds.RasterYSize
        # convert coords to pixel ids
        gt = ds.GetGeoTransform()
        del ds
        minX = gt[0]
        minY = gt[3] + width * gt[4] + height * gt[5]
        # maxX = gt[0] + width * gt[1] + height * gt[2]
        # maxY = gt[3]
        pixel_size = gt[1]  # width, [5] - height
        df.iloc[:, 0] = ((df.iloc[:, 0] - minX) / pixel_size).astype("int32")
        df.iloc[:, 1] = ((df.iloc[:, 1] - minY) / pixel_size).astype("int32")
        # print(df)
        # print(df.iloc[:, 2].value_counts())
        # print(width, height, minX, minY, maxX, maxY)
        assert df.shape[0] == dx*dy

        df.to_parquet(path_parquet, index=False)
        # ext = ".parquet"
        # shutil.move(path_parquet, folder_final)  # doesn't work moving to mounted disk
        #parquet_to_csv(path_parquet, path_csv)
        # df.to_csv(path_csv, index=False)
        # print(time.strftime(time_format) + " Done saving to csv")
        print(f"{time.strftime(time_format)} Size of resulting file is "
              f"{os.path.getsize(path_parquet) / pow(2, 20):.1f} MB")
        # delete used files
        if 1:
            if os.path.exists(path_raster_cropped):
                os.remove(path_raster_cropped)
            if os.path.exists(path_raster):
                os.remove(path_raster)
        # TODO move result to folder_final/... (sudo needed?)
        # update list of files - account for newly updated ones
        # files = glob.glob(os.path.join(folder_to_process, "*.tif"))
    except Exception as exc:
        with open(log_file, 'a+') as f:
            f.write(f"{date} {exc}\n")
        print("EXCEPTION")
        traceback.print_exc()
        return None
    return date


def main(args):
    os.makedirs(folder_temp, exist_ok=True)
    os.makedirs(folder_logs, exist_ok=True)
    os.makedirs(folder_result, exist_ok=True)

    print(time.strftime(time_format))
    dates = []
    files = glob.glob(os.path.join(folder_to_process, "*.tif"))
    print([re.findall(r'\d+', Path(f).stem)[0] for f in files])
    if args.parall_type == "process":
        ParallelExecutor = concurrent.futures.ProcessPoolExecutor(max_workers=args.workers)
    elif args.parall_type == "thread":
        ParallelExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)
    else:
        raise NotImplementedError
    with ParallelExecutor as executor:
        future_to_url = {executor.submit(convert_geotiff_to_csv_parquet, f, ):
                             f for f in files}
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

    # TODO automatic download of raw data and upload of results
    print(f"Done dates {dates}")
    print(f"from {dates[0]} to {dates[-1]}")


if __name__ == '__main__':
    args = parser.parse_args()

    folder_data = r"C:\Users\antonma\RA\data"
    folder_to_process = os.path.join(folder_data, args.type)
    folder_result = os.path.join(folder_data, args.type, "result")
    folder_temp = os.path.join(folder_data, "temp")
    folder_logs = os.path.join(folder_data, args.type, "logs")
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
