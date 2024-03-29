import argparse
import csv
import logging
import os
import platform
import pickle
import subprocess
import time

from qgis_utils import *

parser = argparse.ArgumentParser()
parser.add_argument('--alg_type', type=str, help='algorithm to run', choices=("label", "dist"), default="label")
parser.add_argument('-n', type=int, help='number of programs to run in parallel, divide x-axis', default=1)
parser.add_argument('-m', type=int, help='number of subprograms to run consequently, divide y-axis', default=1)
parser.add_argument('--id_x', type=int, help='id of the program run on x-axis', default=0)
parser.add_argument('--id_y', type=int, help='id of the program run on y-axis', default=0)
# TODO bool doesn't go correctly in run
parser.add_argument('--debug', type=str, help='to run in debug (small) mode or not', default="False")
parser.add_argument('--tilename', type=str, help='name of the file with tiles to get from',
                    default="COMM_RG_01M_2016_4326_fixed.shp")
parser.add_argument('--rewrite_result', default=False, action="store_true",
                    help='whether to rewrite or rather append the resulting csv/xlsx file')
parser.add_argument('--result_name', type=str, help='name of the file with tiles', default="pixel")
parser.add_argument('--code', default=None, type=str, help='name of the country to process', required=True)
parser.add_argument('--lockdown_file', default=None, type=str, help='from which .csv take dates and nuts/comm_id', )
parser.add_argument('--load_data', default=False, action="store_true", help='whether to load layers with tiles or create otherwise',
                    )


def main(args):
    assert args.id_x < args.n
    assert args.id_y < args.m
    # set up paths and names
    if platform.system() == "Linux":
        path_python = "python"
    elif  platform.system() == "Windows":
        path_python = r"C:\ProgramData\Anaconda3\envs\qgis\python.exe"
    folder = os.path.join(os.getcwd(), args.alg_type)
    if args.code == "LAND":
        path_land_use_tiles = os.path.join(os.getcwd(), "land_use", "used_4326.shp")
    folder_tiles = os.path.join(os.getcwd(), "pixel", "tiles")
    # args.tilename = os.path.join(folder_tiles, args.tilename)  # TODO if used simultaneously by several progs?-copy mb?
    tiles_path = os.path.join(folder_tiles, args.tilename)

    country_string = "_".join([str(args.code), (str(args.id_x) + "_" + str(args.id_y))])
    log_string = "_".join([args.alg_type, country_string])
    log_file = os.path.join(folder, "_".join(["log", log_string + ".txt"]))
    result_name = os.path.join(folder, "_".join([args.result_name, log_string + ".csv"]))
    in_border_tiles_path = os.path.join(folder_tiles, "in+border_" + country_string + ".shp")

    crs_name = "epsg:4326"
    crs_code = crs_name.split(":")[-1]

    start = time.time()

    if args.alg_type == "label":
        code_name = "raster_label.py"
        pixel_sizes = [
            40,
            8,
            2,
            1]
        if args.code == "LAND":
            result_header = ['X', 'Y', 'AREA_URB', 'AREA_PERCENT_URB',
                             'AREA_IND', 'AREA_PERCENT_IND',
                             'AREA_ROAD', 'AREA_PERCENT_ROAD']
        else:
            result_header = ['X', 'Y', 'NUTS_CODE', 'COMM_ID', 'AREA', 'AREA_PERCENT']
    elif args.alg_type == "dist":
        code_name = "measure_dist.py"
        pixel_sizes = [1]
        result_header = ['X', 'Y', 'DISTANCE',
                         'CLOSEST_COMM_ID', ]
    else:
        raise NotImplementedError(f"Not known algorithm type {args.alg_type}")

    # create result file with header
    if os.path.exists(result_name):
        os.remove(result_name)
    elif not os.path.exists(folder):
        os.makedirs(folder)
    with open(result_name, "w+", newline='') as file:
        filewriter = csv.writer(file, delimiter=",")
        filewriter.writerow(result_header)
    with open(log_file, 'w+') as f:
        pass

    #  set logging level
    if args.debug == "True":
        logging.basicConfig(format='%(message)s',
                            level=logging.INFO)  # choose between WARNING - INFO - DEBUG
    else:
        logging.basicConfig(format='%(message)s',
                            level=logging.WARNING)  # DEBUG)
    logger = logging.getLogger(__name__)

    logger.setLevel(getattr(logging, "DEBUG"))

    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    logger.addHandler(fh)

    # get tiles and extents for country(ies) by NUTS code
    code = args.code
    nuts = [code, ]
    save_path = os.path.join(folder_tiles, log_string + ".shp")
    print(f"Save results to {result_name}")
    with QGISContextManager():
        # get in-country tiles and also direct neighboring tiles
        if args.code != "LAND":
            if args.load_data:
                path_country = os.path.join(folder_tiles, "label_" + code + "_"
                                            + str(args.id_x) + "_" + str(args.id_y) + ".shp")  # TODO create files with "_" +
                                                                                              # crs_code +
                path_border = os.path.join(folder_tiles, "border_" + code + "_" + crs_code + ".shp")
                print(f"Get country tiles from {path_country}")
                print(f"Get border tiles from {path_border}")
                filtered_tiles = load_layer(path_country)
                ext = filtered_tiles.extent()
                extent = (ext.xMinimum(), ext.xMaximum(),
                          ext.yMinimum(), ext.yMaximum())
                tiles_border = load_layer(path_border)
            else:
                expr = expression_from_nuts_comm(nuts_yes=nuts)  # comm_yes=nuts)
                print(f"Used expression to select the tiles {expr}")
                filtered_tiles, extent, _ = layer_filter_from_expression(tiles_path, expr=expr,
                                                                         crs_name=crs_name,
                                                                         save_flag=False, save_path=save_path, get_extent=True)
                tiles_border, _, _ = get_border_of_country(code, tiles_path, save_flag=False, save_path=None,
                                                           save_name=None,
                                                           crs_name=crs_name)

            # merge country + her neighboring tiles
            tiles_to_label = merge_two_vector_layers(filtered_tiles, tiles_border)
            export_layer(tiles_to_label, save_path=in_border_tiles_path)
            print(f"Extents of country {extent}")
            print(f"Saved tiles to {in_border_tiles_path}")
        # TODO: for further optimization if several programs divide the whole area, take only tiles intersecting with
        #  the current part
            (x0, x1, y0, y1) = get_sizes_in_pixels_from_degrees(extent)
        else:
            in_border_tiles_path = path_land_use_tiles
            (x0, x1, y0, y1) = (40272, 49921, 2808, 10441)  # from tiff_to_csv.py
    if args.code == "FR":  # otherwise takes also islands far away from the continent
        x0 = 41000
        x1 = 46000
        y0 = 5000
        y1 = 8500
    start_x = x0 + (x1 - x0) * args.id_x // args.n
    end_x = x0 + (x1 - x0) * (args.id_x + 1) // args.n
    start_y = y0 + (y1 - y0) * args.id_y // args.m
    end_y = y0 + (y1 - y0) * (args.id_y + 1) // args.m
    print(f"x: {start_x} - {end_x}, y: {start_y} - {end_y}")
    x_times = math.ceil((end_x - start_x) / float(pixel_sizes[0]))  # TODO exact calculations
    y_times = math.ceil((end_y - start_y) / float(pixel_sizes[0]))
    total_pixels = x_times * y_times * pixel_sizes[0] ** 2  # (end_x - start_x) * (end_y - start_y)

    ##################################################################################3
    if args.alg_type == "label":
        global_count = 0
        #  iterate over pixels in rectangular zone of input raster
        times = [time.time()]
        count = 0
        for k1, i in enumerate(range(start_x, end_x, pixel_sizes[0])):
            for k2, j in enumerate(range(start_y, end_y, pixel_sizes[0])):
                done_pixels = (k1 * y_times + k2) * pixel_sizes[0] ** 2
                print(f"Started {k1} {k2} of {x_times-1} {y_times-1}, "
                      f"country {args.code},",
                      f"done from {total_pixels // 1000} K -- "
                      f"{done_pixels // 1000} K",
                      f"{done_pixels / total_pixels * 100:.2} %")
                command = [path_python, code_name,
                           "--x0", str(i),
                           "--y0", str(j),
                           "--tilepath", str(in_border_tiles_path),
                           "--result_name", str(result_name),
                           "--debug", str(args.debug),
                           "--code", str(args.code)]
                count += 1
                times.append(time.time())
                print(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())}, "
                      f"global speed per 1 Mpixel {((time.time() - start) / count / (pixel_sizes[0] ** 2) * 10 ** 6):.2}s, "
                      f"local speed per 1 Mpixel  {((times[-1] - times[-2]) / (pixel_sizes[0] ** 2) * 10 ** 6):.2}s, "
                      f"processed pixels: {count * pixel_sizes[0] ** 2 // 10 ** 6} M "
                      f"{count * pixel_sizes[0] ** 2 % 10 ** 6 // 10 ** 3} K")
                try:
                    out = subprocess.run(command,
                                         capture_output=True)  # TODO: possibility to kill everything with Ctrl+C
                    print(out.stdout.decode('ascii'))
                    if out.returncode != 0:
                        with open(log_file, 'a+') as f:
                            f.write(f"{i} {j}\n")
                            f.write(out.stderr.decode('ascii'))
                            print(f"ERROR with {i} {j}")
                except:
                    with open(log_file, 'a+') as f:
                        f.write(f"{i} {j}\n")
                    logging.exception()
                    print("EXCEPTION")

    elif args.alg_type == "dist":
        print("dist not working yet")
        return 0
        i = 0
        j = 0
        command = [path_python, code_name,
                   "-n", str(args.n_all),  # overall x-tiles, e.g. 60
                   "-m", str(args.m),  # overall x-tiles, e.g. 64
                   "--id_x", str(j),  # current x-tile
                   "--id_y", str(i),  # current y-tile
                   # "--rewrite_result", str(rewrite),
                   "--tilepath", str(args.tilename),  # + str(args.id_x_curr),
                   "--result_name", result_name,
                   "--debug", str(args.debug),
                   "--country", str(args.code),
                   "--lockdown_file", str(args.lockdown_file)]

    try:
        out = subprocess.run(command, capture_output=True)  # TODO: possibility to kill everything with Ctrl+C
        print(out.stdout.decode('ascii'))  # TODO: stdout and stderr to some file too, for later check
        if out.returncode != 0:
            with open(log_file, 'a+') as f:
                f.write(f"{i} {j}\n")
                f.write(out.stderr.decode('ascii'))
                print(f"ERROR with {i} {j}")
    except:
        with open(log_file, 'a+') as f:
            f.write(f"{i} {j}\n")
        print("EXCEPTION")
    end = time.time()
    print("Elapsed time {} minutes".format((end - start) / 60.0))


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
