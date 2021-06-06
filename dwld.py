import os
import requests
import json
import time
import datetime
import pickle
import re
import argparse
import shutil
import concurrent.futures

from bs4 import BeautifulSoup

# arguments
parser = argparse.ArgumentParser()
parser.add_argument('--root', type=str, help='password to https://eogdata.mines.edu/nighttime_light',
                    default="/home/anton/COVID19_Remote")
parser.add_argument('--data_type', type=str, choices=("monthly", "nightly"), help='type of data to download',
                    default="nightly" )
parser.add_argument('--sub_type', type=str, choices=("cloud_cover", "rade9d", None), help='subtype of data to download',
                    default="rade9d")
parser.add_argument('--time_min', type=str, help='start date of files to download', default="20180101")
parser.add_argument('--time_max', type=str, help='end date of files to download (included)',
                    default=datetime.datetime.today().strftime('%Y%m%d'))
parser.add_argument('--user', type=str, help='username for https://eogdata.mines.edu/nighttime_light',
                    default="")
parser.add_argument('--pwd', type=str, help='password to https://eogdata.mines.edu/nighttime_light',
                    default="")
parser.add_argument('--workers', type=int, help='how many processes/threads to use',
                    default="4")
parser.add_argument('--parall_type', type=str, choices=("process", "thread"), help='use processes/threads',
                    default="thread")


def get_token(username, password):
    """Retrieve access token"""
    params = {
        'client_id': 'eogdata_oidc',
        'client_secret': '368127b1-1ee0-4f3f-8429-29e9a93daf9a',
        'username': username,
        'password': password,
        'grant_type': 'password'
    }
    token_url = 'https://eogauth.mines.edu/auth/realms/master/protocol/openid-connect/token'
    response = requests.post(token_url, data=params)
    access_token_dict = json.loads(response.text)
    access_token = access_token_dict.get('access_token')
    return access_token


def save_token(token, filename):
    with open(filename, 'wb') as f:
        pickle.dump(token, f)


def load_token(filename, username=None, password=None):
    """Get token (could load but don't know when expires)"""
    if os.path.isfile(filename):
        pass
        # with open(filename, 'rb') as f:
        #    return pickle.load(f)

    token = get_token(username, password)
    save_token(token, filename)
    return token


def get_all_links(data_type, data_url, headers, session, links_all):
    response = session.get(data_url, headers=headers)
    print(data_url)
    soup = BeautifulSoup(response.text, "lxml")
    # print(soup.prettify())
    links = []
    for link in soup.find_all("a"):
        l = link.get("href")
        if l[0] != "?":
            if not links or links[-1] != l:
                links.append(l)
    # print(links)
    for l in links:
        if l[0:5] != "SVDNB":
            if l[0:2] == "20" and l[2:4] >= "18" or l[0:2] != "20":  # change filter if needed
                get_all_links(data_type, data_url + "/" + l, headers, session, links_all)
        else:
            if data_type == "nightly" or data_type == "monthly" and "75N060W" in l:
                links_all.append(data_url + "/" + l)
    return links_all


def download_url(url, save_path_2_level, session, headers, check_size=False):
    """Download from url to output with checking that if the file exists it has the needed size"""
    # Get name of the folder - save_type
    i = -2
    while not url.split("/")[i]:
        i -= 1
    save_type = url.split("/")[i]  # cloud_cover, rade9d, vcmcfg or vcmlscfg

    save_path = os.path.join(save_path_2_level, save_type)
    os.makedirs(save_path, exist_ok=True)
    output_file = os.path.join(save_path, url.split("/")[-1])
    if not os.path.isfile(output_file) or check_size:
        with session.get(url, headers=headers, stream=True) as response:
            if response.status_code == 200:
                size = int(response.headers.get('content-length'))
                print("Processing {}".format(output_file))
                if os.path.isfile(output_file):
                    if os.path.getsize(output_file) == size:
                        return 0
                    else:
                        os.remove(output_file)
                        print(f"Removed partially downloaded {output_file}")
                t = time.strftime("%H:%M:%S", time.localtime())
                print("{} Downloading {} of size {:.2f} MB".format(t, url, size / pow(2, 20)), end="\n")
                st1 = time.time()
                with open(output_file, 'wb') as f:
                    shutil.copyfileobj(response.raw, f)
                t = time.strftime("%H:%M:%S", time.localtime())
                print("{} Downloaded {} speed {:.2f} MB/s".format(t, url, size / pow(2, 20) / (time.time() - st1)))
            else:
                print("Failed with {}, status {}".format(url, response.status_code))
        return output_file
    else:
        return 0


def main(args):
    # TODO: get from config file
    if args.user is None:
        args.user = "antonma@student.ethz.ch"
    if args.pwd is None:
        args.pwd = "EJXYY8tmDC6crMw"

    if args.data_type == "monthly":
        args.sub_type = "v10"

    data_url = "https://eogdata.mines.edu/nighttime_light/" + args.data_type + "/" + args.sub_type

    time_max = datetime.datetime.today().strftime('%Y%m%d')

    start = time.time()

    # Submit request with token bearer
    access_token = load_token(os.path.join(args.root, "token.dat"), username=args.user, password=args.pwd)
    auth = 'Bearer ' + access_token
    headers = {'Authorization': auth}

    # get all the downloadable links from subdirectories of data_url
    session = requests.Session()
    links = get_all_links(args.data_type, data_url, headers, session, [])

    # extract times and filter
    times = [re.findall(r'\d+', l.split("/")[-1])[0] for l in links]
    links_dwn = [l for i, l in enumerate(links) if args.time_min <= times[i] <= args.time_max]
    #links_dwn = links_dwn[0:5]
    print('Overall files: {}, to download {}'.format(len(links), len(links_dwn)))

    save_path_2_level = os.path.join(args.root, "01_raw_data", args.data_type)
    # Download/check urls in parallel with threads
    if args.parall_type == "process":
        ParallelExecutor = concurrent.futures.ProcessPoolExecutor(max_workers=args.workers)
    elif args.parall_type == "thread":
        ParallelExecutor = concurrent.futures.ThreadPoolExecutor(max_workers=args.workers)
    else:
        raise NotImplementedError
    with ParallelExecutor as executor:
        future_to_url = {executor.submit(download_url, url, save_path_2_level, session, headers, check_size=True):
                             url for url in links_dwn}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
        try:
            res = future.result()
        except Exception as exc:
            print('%r generated an exception: %s' % (url, exc))
        else:
            print('%r page is %d bytes' % (url, res))

    end = time.time()
    print("Elapsed time {} minutes".format((end - start) / 60.0))
    # print("Average speed {} MB/s".format(sum(sizes)/(end - start)))


if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
