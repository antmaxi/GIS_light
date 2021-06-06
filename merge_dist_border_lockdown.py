import time

import pandas as pd
import argparse
import os
import glob
import logging

import config

parser = argparse.ArgumentParser()
parser.add_argument('--code', type=str, help='name of the country to process', default=None)

logging.basicConfig(format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("merge")
logger.setLevel(getattr(logging, "INFO"))


def cont_quest():
    return 0
    assert input("Continue? If yes, type 'y', else smth different\n") == "y", "Aborted by user"


def main(args):
    """
        Merge and lockdown data about pixels in one file
    """
    folder_lockdown = os.path.join(config.folder_dist_save, "raw")
    folder_border = os.path.join(config.folder_dist_save)
    assert args.code is not None
    pattern_lockdown = os.path.join(folder_lockdown, ("*" + "dist_lockdown_" + args.code + "*"))
    pattern_border = os.path.join(folder_border, ("dist_border_" + args.code + "*"))
    n_rows = []
    for ind, f in enumerate(glob.glob(pattern_lockdown)):
        print(f)

    result_columns = ['X', 'Y', "COUNTRY_CODE", "NUTS_CODE", "COMM_ID"] \
                     + ['DIST_LOCKDOWN_KM', "NEAREST_COMM_ID_LOCKDOWN", "NEAREST_NUTS_LOCKDOWN", ] \
                     + ["isLockdown", ] \
                     + ['DIST_BORDER_KM', "NEAREST_COMM_ID_BORDER", "NEAREST_NUTS_BORDER",
                        "NEAREST_COUNTRY_CODE_BORDER"]
    df0 = pd.DataFrame(columns=result_columns)
    for ind_l, f_l in enumerate(
            glob.glob(pattern_lockdown)):  # TODO optimize, e.g. do several lckdwn with the same border
        n_rows = []
        logger.info(f"Processing {f_l}")
        result_name = f_l.replace("lockdown", "border_lockdown")
        print(f"\nWill create file {result_name}")
        if os.path.exists(result_name):
            print("File exists")
            return 1
        cont_quest()
        df_l = pd.read_csv(f_l, header=0)
        # TODO better concatenation, e.g. first sort pixels "X" and "Y", then just do horizontal concat
        for ind_b, f_b in enumerate(glob.glob(pattern_border)):
            print(f_b)
            start_time = time.time()
            rows = []
            df_b = pd.read_csv(f_b, header=0)  # if ind == 0 else 1)
            k = 0
            print(df_b["X"].tolist()[0:10])
            print(df_b["X"].tolist()[0:10])
            assert df_b["X"].tolist() == df_l["X"].tolist() and df_b["Y"].tolist() == df_l["Y"].tolist()
            for index, row_b in df_b.iterrows():  # ['X', 'Y', "COUNTRY_CODE", "NUTS_CODE", "COMM_ID",
                k += 1
                # 5: 'DIST_LOCKDOWN_KM', "NEAREST_COMM_ID", "NEAREST_NUTS", "NEAREST_COUNTRY_CODE"]
                row_l = df_l.loc[(df_l['X'] == row_b['X']) & (df_l['Y'] == row_b['Y'])]
                assert row_l.shape[0] == 1
                row_l = row_l.iloc[0]
                #print(row_l.tolist())
                row_res = row_l.tolist()[1:9] + [row_l.tolist()[10], ] + row_b.tolist()[5:9]
                #print(row_res)
                rows.append(row_res)
                if k % 100000 == 0:
                    print(f"{time.strftime('%m/%d/%Y, %H:%M:%S', time.localtime())} k={k}")
            #print(rows)
            df_res = pd.DataFrame(rows, columns=result_columns)
            #print(df_res)
            cont_quest()
            df_res.to_csv(result_name, mode="w+", index=False)
            print(f"Elapsed {time.time() - start_time:.0f} s")
            print(f"Total rows {len(df_res)}")
            #df_res = pd.read_csv(result_name, header=0)
        # assert sum(n_rows) == len(df)


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
