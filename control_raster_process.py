import os
import subprocess
import argparse
import pickle
import csv

parser = argparse.ArgumentParser()
parser.add_argument('--alg_type', type=str,  help='algorithm to run', choices=("label", "dist"))
parser.add_argument('-n', type=int,  help='number of programs to run in parallel, divide x-axis', default=1)
parser.add_argument('--n_all', type=int,  help='how many tiles are overall on x-axis, to divide between programs',
                    default=1)
parser.add_argument('-m', type=int,  help='number of subprograms to run consequently, divide y-axis', default=1)
parser.add_argument('--id_x_curr', type=int,  help='id of the program run on x-axis', default=0)
parser.add_argument('--debug', type=bool, help='to run in debug (small) mode or not', default=False)
parser.add_argument('--tilename', type=str, help='name of the file with tiles', default="France")
parser.add_argument('--rewrite_result', type=bool,
                    help='whether to rewrite or rather append the resulting csv/xlsx file', default=False)
parser.add_argument('--result_name', type=str, help='name of the file with tiles', default="pixel")
parser.add_argument('--country', type=str, help='name of the country to process', default=None)

def main(args):
    # set up paths and names
    path_python = r"C:\ProgramData\Anaconda3\envs\qgis\python.exe"
    folder = os.path.join(os.getcwd(), args.alg_type)
    log_file = os.path.join(folder,
                            "_".join(["log", args.alg_type, str(args.country), (str(args.id_x_curr) + ".txt")]))
    result_name = os.path.join(folder,
                               "_".join([args.result_name, args.alg_type, str(args.country),
                                                 (str(args.id_x_curr) + ".csv")]))
    tilename = str(args.tilename) + str(args.id_x_curr)
    if args.alg_type == "label":
        code_name = "raster_label.py"
        result_header = ['X', 'Y', 'NUTS_CODE', 'COMM_ID', 'AREA', 'AREA_PERCENT']
    elif args.alg_type == "dist":
        code_name = "measure_dist.py"
        result_header = ['X', 'Y', 'DISTANCE', 'CLOSEST_COMM_ID',]  # TODO add smth else
    else:
        raise NotImplementedError(f"Not known algorithm type {args.alg_type}")

    # create result file with header
    if os.path.exists(result_name):
        os.remove(result_name)
    if not os.path.exists(folder):
        os.makedirs(folder)
    with open(result_name, "w+", newline='') as file:
        filewriter = csv.writer(file, delimiter=",")
        filewriter.writerow(result_header)
    with open(log_file, 'w+') as f:
        pass

    for i in range(args.m):
        num_x_tiles = args.n_all // args.n
        for j in range(num_x_tiles*args.id_x_curr, num_x_tiles*(args.id_x_curr+1), ):
            if args.alg_type == "label":
                #if (i > 17) or (i == 17) and (j >= 36):
                print(f"Started {i} {j} of {args.m} {args.n_all}, "
                      f"done from {num_x_tiles*args.m * 1600 // 1000} K -- "
                      f"{(i*num_x_tiles + j - num_x_tiles*args.id_x_curr) * 1600 // 1000} K")
                command = [path_python, code_name,
                                 "-n", str(args.n_all),  # overall x-tiles, e.g. 60
                                 "-m", str(args.m),  # overall x-tiles, e.g. 64
                                 "--id_x", str(j),  # current x-tile
                                 "--id_y", str(i),  # current y-tile
                                 #"--rewrite_result", str(rewrite),
                                 "--tilename", tilename,# + str(args.id_x_curr),
                                 "--result_name", result_name,
                                 "--debug", str(args.debug),
                                 "--country", str(args.country)]
            elif args.alg_type == "dist":
                command = [path_python, code_name, ]  # TODO add parameters
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
            #print(res)
            #assert 0 == 0



if __name__ == '__main__':
    args = parser.parse_args()
    main(args)