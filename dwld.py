import os
import requests
import json
import time
import pickle
import re
import argparse
import shutil
from concurrent.futures import ThreadPoolExecutor


# import pandas as pd

from bs4 import BeautifulSoup

parser = argparse.ArgumentParser()


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
        #with open(filename, 'rb') as f:
        #    return pickle.load(f)

    token = get_token(username, password)
    save_token(token, filename)
    return token


def get_all_links(data_type, data_url, headers, session, links_all):
    response = session.get(data_url, headers=headers)
    print(data_url)
    soup = BeautifulSoup(response.text, "lxml")
    #print(soup.prettify())
    links = []
    for link in soup.find_all("a"):
        l = link.get("href")
        if l[0] != "?":
            if not links or links[-1] != l:
                links.append(l)
    #print(links)
    for l in links:
        if l[0:5] != "SVDNB":
            if l[0:2] == "20" and l[2:4] >= "20" or l[0:2] != "20":
                get_all_links(data_type, data_url + "/" + l, headers, session, links_all)
        else:
            if data_type == "nightly" or data_type == "monthly" and "75N060W" in l:
                links_all.append(data_url + "/" + l)
    return links_all

# TODO: parallel downloading
def download_url(url, output_file, session, headers):
    if not os.path.isfile(output_file):
        with session.get(url, headers=headers, stream=True) as response:
            if response.status_code == 200:
                size = int(response.headers.get('content-length')) / pow(2, 20)
                t = time.strftime("%H:%M:%S", time.localtime())
                print("{} Downloading {} of size {:.2f} MB".format(t, url, size), end="")
                st1 = time.time()
                with open(output_file, 'wb') as f:
                    shutil.copyfileobj(response.raw, f)
                print(", speed {:.2f} MB/s".format(size / (time.time() - st1)))
            else:
                print("Failed with {}, status {}".format(url, response.status_code))
        return output_file
    else:
        return 0

def main(args):
    username = "antonma@student.ethz.ch"
    password = "EJXYY8tmDC6crMw"
    root_path = "/media/anton/Transcend/COVID19_Remote" # "/home/anton/COVID19_Remote" #
    data_type = "nightly" #"monthly" #
    sub_type = "rade9d" # "cloud_cover" #
    if data_type == "monthly":
        sub_type = "v10"

    data_url = "https://eogdata.mines.edu/nighttime_light/" + data_type + "/" + sub_type

    if sub_type == "cloud_cover":
        time_min = "20200226"  # "20180101"
    elif sub_type == "rade9d":
        time_min = "20180212"
    else:
        time_min = "20180112"
    time_max = "20201231"

    start = time.time()

    # Submit request with token bearer
    access_token = load_token(os.path.join(root_path, "token.dat"), username=username, password=password)
    auth = 'Bearer ' + access_token
    headers = {'Authorization': auth}

    # get all the downloadable links from subdirectories of data_url
    session = requests.Session()
    links = get_all_links(data_type, data_url, headers, session, [])
    print('Overall files: {}'.format(len(links)))

    # extract times and filter
    times = [re.findall(r'\d+', l.split("/")[-1])[0] for l in links]
    links_dwn = [l for i, l in enumerate(links) if times[i] >= time_min and times[i] <= time_max]
    #print(times)

    sizes = []
    for l in links_dwn:
        i = -2
        while not l.split("/")[i]:
            i -= 1
        save_type = l.split("/")[i]  # cloud_cover, rade9d, vcmcfg or vcmlscfg
        save_path = os.path.join(root_path, "01_raw_data", data_type, save_type)
        print(save_path)
        os.makedirs(save_path, exist_ok=True)
        output_file = os.path.join(save_path, l.split("/")[-1])
        #results = ThreadPool(8).imap_unordered(fetch_url, urls)
        download_url(l, output_file, session, headers)

    end = time.time()
    print("Elapsed time {} minutes".format((end - start)/60.0))
    #print("Average speed {} MB/s".format(sum(sizes)/(end - start)))
    # TODO: add check of file size

if __name__ == '__main__':
    args = parser.parse_args()
    main(args)
