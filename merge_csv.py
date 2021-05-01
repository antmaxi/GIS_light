import pandas as pd
import argparse
import os
import glob

parser = argparse.ArgumentParser()
parser.add_argument('--result_name', type=str, help='name of the file with tiles', default="None")
parser.add_argument('--country', type=str, help='name of the country to process', default=None)
parser.add_argument('--pattern', type=str, help='relative pattern of files to process', default=None)

def main(args):
    folder = os.path.join(os.getcwd(), "saved")  #"pixel")
    if args.country is not None and args.pattern is None:
        args.pattern = os.path.join(folder,
                                   ("*" + args.result_name + "_"
                                    + str(args.country) + "_"
                                    + "*" + ".csv"))
    if args.country is not None:
        name = args.country
    else:
        name = "labels_"
    if args.pattern is None:
        args.pattern = "*_" + args.country + "_*.csv"
    if args.country is None:
        name += args.pattern.split("*")[-2]
    name += ".csv"
    result_name = os.path.join(folder, name)
    if os.path.exists(result_name):
        print("File exists")
        return 1

    df0 = pd.DataFrame(columns=['X', 'Y', 'NUTS_CODE', 'COMM_ID', 'AREA', 'AREA_PERCENT'])
    list_of_dfs =[df0,]
    n_rows = []
    for ind, f in enumerate(glob.glob(os.path.join(os.getcwd(), args.pattern))):
        print(f)
    print(f"\nWill create file {result_name}")
    answer = input("Continue? If yes, type 'y', else smth different\n")
    if answer != "y":
        print("Aborted")
        return 1

    for ind, f in enumerate(glob.glob(os.path.join(os.getcwd(), args.pattern))):
        print(f)
        df = pd.read_csv(f, header=0)  # if ind == 0 else 1)
        list_of_dfs.append(df)
        n_rows.append(len(list_of_dfs[-1]))
        print(f"Read {f} rows - {n_rows[-1]}")
        print(df.columns)
    result = pd.concat(list_of_dfs, ignore_index=True)
    result.to_csv(result_name, mode="w+")

    df = pd.read_csv(result_name, header=1)
    #print(sum(n_rows))
    print(f"Total rows {len(df)}")
    assert (sum(n_rows) - 1) == len(df)

if __name__ == '__main__':
    args = parser.parse_args()
    main(args)