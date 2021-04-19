import os
import subprocess
import argparse
import pickle
import csv

parser = argparse.ArgumentParser()
parser.add_argument('-n', type=int,  help='number of programs to run in parallel, divide x-axis', default=1)
parser.add_argument('--n_all', type=int,  help='how many tiles are overall on x-axis, to divide between programs',
                    default=1)
parser.add_argument('-m', type=int,  help='number of subprograms to run consequently, divide y-axis', default=1)
parser.add_argument('--id_x_curr', type=int,  help='id of the program run on x-axis', default=0)
parser.add_argument('--debug', type=bool, help='to run in debug (small) mode or not', default=False)
parser.add_argument('--tilename', type=str, help='name of the file with tiles', default="France")
parser.add_argument('--rewrite_result', type=bool,
                    help='whether to rewrite or rather append the resulting csv/xlsx file', default=False)
parser.add_argument('--result_name', type=str, help='name of the file with tiles', default="pixel_intersections")
parser.add_argument('--country', type=str, help='name of the country to process', default=None)

def main(args):
    path = r"C:\ProgramData\Anaconda3\envs\qgis\python.exe"
    code_name = "raster_label.py"
    log_pixel = "log_pixel_" +  str(args.country) + "_" + str(args.id_x_curr) + ".txt"
    folder = os.path.join(os.getcwd(), "pixel")
    result_name = os.path.join(folder, (args.result_name + "_" + str(args.country) + "_" + str(args.id_x_curr) + ".csv"))
    tilename = str(args.tilename) + str(args.id_x_curr)
    result_header = ['X', 'Y', 'NUTS_CODE', 'COMM_ID', 'AREA', 'AREA_PERCENT']
    if 1:
        if os.path.exists(result_name):
            os.remove(result_name)
        with open(result_name, "w+", newline='') as file:
            filewriter = csv.writer(file, delimiter=",")
            filewriter.writerow(result_header)
    with open(log_pixel, 'w+') as f:
        pass

    for i in range(args.m):
        num_x_tiles = args.n_all // args.n
        for j in range(num_x_tiles*args.id_x_curr, num_x_tiles*(args.id_x_curr+1), ):
            #if (i > 17) or (i == 17) and (j >= 36):
            print(f"Started {i} {j} of {args.m} {args.n_all}, "
                  f"done from {num_x_tiles*args.m * 1600 // 1000} K -- "
                  f"{(i*num_x_tiles + j - num_x_tiles*args.id_x_curr) * 1600 // 1000} K")
            try:
                subprocess.run([path, code_name,
                             "-n", str(args.n_all),  # overall x-tiles, e.g. 60
                             "-m", str(args.m),  # overall x-tiles, e.g. 64
                             "--id_x", str(j),  # current x-tile
                             "--id_y", str(i),  # current y-tile
                             #"--rewrite_result", str(rewrite),
                             "--tilename", tilename,# + str(args.id_x_curr),
                             "--result_name", result_name,
                             "--debug", str(args.debug),
                             "--country", str(args.country)])
            except:
                with open(log_pixel, 'a+') as f:
                    f.write(f"{i} {j}\n")
                print("EXCEPTION")
            #print(res)
            #assert 0 == 0



if __name__ == '__main__':
    args = parser.parse_args()
    main(args)