import pandas as pd
import argparse
import os
import glob
import logging

import config

parser = argparse.ArgumentParser()
#parser.add_argument('--result_name', type=str, help='name of the file with tiles', default="None")
parser.add_argument('--code', type=str, help='name of the country to process', default=None)
#parser.add_argument('-l', '--list', nargs='+', help='<Required> List of files to merge', required=Tru)


logging.basicConfig(format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("merge")
logger.setLevel(getattr(logging, "INFO"))


def cont_quest():
    return 0
    assert input("Continue? If yes, type 'y', else smth different\n") == "y", "Aborted by user"


def main(args):
    folder = os.path.join(config.folder_dist_save, "raw")
    assert args.code is not None
    pattern1 = os.path.join(folder, ("*" + "from_lockdown_" + args.code + "*"))

    n_rows = []
    for ind, f in enumerate(glob.glob(pattern1)):
        print(f)
    dates = []

    def check_to_from(file, dfnow):
        if file.find("to") != -1:
            dfnow["isLockdown"] = 0
        elif file.find("from") != -1:
            dfnow["isLockdown"] = 1
        else:
            raise NameError(f"Not found 'to' or 'from' in the filename {f}")
        return dfnow

    for ind, f in enumerate(glob.glob(pattern1)):
        date = f.split("_")[-2]
        if date in dates:
            continue
        df0 = pd.DataFrame(columns=(config.dist_header + ["isLockdown",]))
        list_of_dfs = [df0, ]
        n_rows = []
        dates.append(dates)
        logger.info(f"Processing {f}")
        logger.info(f"Date {date}")
        result_name = f.rstrip("_" + f.split("_")[-1]) + ".csv"
        result_name = result_name.replace("_to_", "_")
        result_name = result_name.replace("_from_", "_")
        print(f"\nWill create file {result_name}")
        if os.path.exists(result_name):
            print("File exists")
            return 1
        cont_quest()
        df = pd.read_csv(f, header=0)
        df = check_to_from(f, df)
        list_of_dfs.append(df)
        n_rows.append(len(list_of_dfs[-1]))
        # find second and further file for the same date and country
        for ind2, f2 in enumerate(glob.glob(os.path.join(folder, ("*" + args.code + "*")))):
            date2 = f2.split("_")[-2]
            if date2 != date or f2 == f:
                #print(f"skip {f2}")
                continue
            print(f2)
            df2 = pd.read_csv(f2, header=0)  # if ind == 0 else 1)
            df2 = check_to_from(f2, df2)
            #print(df.iloc[0])
            list_of_dfs.append(df2)
            n_rows.append(len(list_of_dfs[-1]))
            #print(f"Read {f2} rows - {n_rows[-1]}")
            cont_quest()
        result = pd.concat(list_of_dfs, ignore_index=True)
        result = result.drop_duplicates(subset=['X', 'Y'], keep='last')
        result.to_csv(result_name, mode="w+")

        df = pd.read_csv(result_name, header=0)
        print(f"Total rows {len(df)}")
        #assert sum(n_rows) == len(df)


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
