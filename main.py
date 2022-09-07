#!/usr/bin/python3
import csv, os, errno
import sys, json
import getopt, re, requests
from typing import Pattern
from urllib.parse import urljoin
from datetime import date
import settings
import upload
try:
    opts, args = getopt.getopt(sys.argv[1:], 'i:f:t:m:h', ['input=', 'filter=', 'top=', 'market=', 'help'])
except getopt.GetoptError:
    sys.exit(2)
opt_dict = {
    'inp_array': [],
    'global_f': None,
    'global_m': None,
    'global_t': None,
    'global_k': None
}
today = date.today()
current_qtr = (today.month - 1) // 3 + 1
current_qtr_yr = "{}{:02d}".format(today.strftime("%Y"), current_qtr)
def create_directory(dir_name):
    if not os.path.exists(dir_name):
        print("[INFO] Directory not existed trying to create '{}'".format(dir_name))
        try:
            os.makedirs(dir_name)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
            sys.exit(2)
# pyhon3 scipt.py -k k1,k2,k3  -f f1,,f3 -m ,,m3
# pyhon3 scipt.py -v k1:f1:m1:t1, k2, m2
for opt, arg in opts:
    if opt in ('-h', '--help'):
        sys.exit(2)
    elif opt in ('-i', '--input'):
        opt_dict['inp_array'] = arg.split(',')
    elif opt in ('-f', '--filter'):
        opt_dict['global_f'] = arg
    elif opt in ('-t', '--top'):
        opt_dict['global_t'] = arg
    elif opt in ('-m', '--market'):
        opt_dict['global_m'] = arg
    else:
        sys.exit(2)
def get_auth_token():
    ###
    tenant_id = settings.TENANT_ID
    client_id = settings.CLIENT_ID
    client_secret = settings.CLIENT_SECRET
    scope = settings.SCOPE
    grant_type = "client_credentials"
    data = {
        "grant_type": grant_type,
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": scope
    }
    auth_url = settings.AUTH_URL
    print("[INFO] Token url  '{}'".format(auth_url))
    # try the request 3 times
    for x in range(1, 4):
        auth_response = requests.post(auth_url, data=data)
        if auth_response.status_code == 200:
            break
        else:
            print("[ERROR] TRY:{} Invalid response from url auth_url \n error_response{}".format(x, auth_response.text))
    # token not found exit
    if auth_response.status_code != 200:
        print("[ERROR] exiting")
        sys.exit(2)
    # Read token from auth response
    auth_response_json = auth_response.json()
    print(auth_response_json)
    auth_token = auth_response_json["access_token"]
    auth_token_header_value = "Bearer %s" % auth_token
    auth_token_header = {"Authorization": auth_token_header_value, 'Accept': 'application/json',
                         'Content-Type': 'application/json'}
    print("Auth Token obtained successfully")
    return auth_token_header
BASE_URL = "https://<SOME_HOST>/api/"  # abc or data

#custom pattern def added here
def get_pattern_conf():
    quater_regx = "\d{4}(0[369]|12)";
    prev_month_filter = today.strftime('%YP%m')
    pre_month = str(today.month - 1)
    prev_month_filter = prev_month_filter[:-2] + pre_month
    pattern_key = {
        'YYY': {'pattern': re.compile("\d{4}P(0[0-9]|1[012])"), 'default': prev_month_filter},
        'XYZ': {'pattern': re.compile(quater_regx), 'default': current_qtr_yr},
        'ABC': {'pattern': re.compile(quater_regx), 'default': current_qtr_yr},
    }
    return pattern_key
#in case of any fiter put config here
def get_filter_conf():
    no_filter_keys = {
        'LE': 1,
        'METADATA_ct': 1
    }
    return no_filter_keys

#in case of specific url def put here
def get_url_conf():
    url_conf = {
        'xyz': 'abc',
        'METADATA_xxxxxxt': 'data'
    }
    return url_conf
def is_valid_key_rule(url_conf):
    pattern_key = get_pattern_conf()
    no_filter_keys = get_filter_conf()
    key = url_cnf.get('k')
    pattern = None
    pattern = pattern_key.get(key)
    if pattern:
        pattern = pattern.get('pattern')
    print("Pattern:", pattern, pattern_key)
    fltr = url_conf.get('f')
    no_pattern = no_filter_keys.get(key)
    default_filter_pattern = None
    df_ptrn = pattern_key.get(key)
    if df_ptrn:
        default_filter_pattern = df_ptrn['default']
    # key need no pattern set everything else none
    if no_pattern:
        print("[INFO] key '{}' need no filter".format(key))
        # set the other values null
        url_conf['f'] = None
        url_conf['m'] = None
        url_conf['t'] = None
        return 1
    # key need pattern and patter provided
    if fltr != None and pattern != None:
        if pattern.match(fltr):
            print("[INFO] key '{}' filter '{}' is valid".format(key, fltr))
            return 1
        else:
            print("[ERROR] Skipped key '{}' filter '{}' combination rule violated check key or filter value".format(key,
                                                                                                                    fltr))
            return 0
    else:
        # get default
        # filter not provide apply default if present
        if default_filter_pattern != None:
            print("[INFO] Default pattern set for key '{}' pattern '{}'".format(key, default_filter_pattern))
            url_conf['f'] = default_filter_pattern
        return 1
auth_header = get_auth_token()
out_url_conf = []
for inp in opt_dict['inp_array']:
    # k=K1:f=F1:t=T1:m=M1
    # k=K2:f=F1
    for split_inp in inp.split(','):
        # k=K1 # f=F1 # t=T1 # m=M1
        url_conf = {'k': None, 'f': None, 'm': None, 't': None}
        for url_key in url_conf:
            for field in split_inp.split(':'):
                # k, K1 # f,F1 # t,T1 # m,M1
                inp_key, inp_val = field.split('=')
                if inp_key == url_key:
                    url_conf[url_key] = inp_val
                elif inp_key != url_key and url_conf.get(url_key) is None:
                    # set default one if given
                    url_conf[url_key] = opt_dict.get('global_' + url_key)
        # append url conf to array
        out_url_conf.append(url_conf)
# headers = {
#   'Content-Type': 'application/json',
#   'Cookie': 'asid=7558a92b63853c979da96ddaa027af12d9d9bd47'
# }
for url_cnf in out_url_conf:
    if is_valid_key_rule(url_cnf):
        key_name = url_cnf.get('k')
        file_name = key_name
        # url = "{BASE_URL}/{key}/{filter}/{market}/{top}".format(BASE_URL=BASE_URL,)
        url_conf = get_url_conf()
        url_part = url_conf.get(key_name)
        url = BASE_URL + '/' + url_part + '/' + url_cnf.get('k')
        if url_cnf.get('f'):
            file_name = file_name + url_cnf.get('f')
            url = url + '/' + url_cnf.get('f')
            if url_cnf.get('m'):
                url = url + '/' + url_cnf.get('m')
                if url_cnf.get('t'):
                    url = url + '/' + url_cnf.get('t')
        print("[INFO] API get for key '{}' filter '{}' url => '{}' ".format(url_cnf.get('k'), url_cnf.get('f'), url))
        # empty variable
        response = None
        for x in range(1, 4):
            # response = requests.request("GET", url, headers=auth_header)
            response = requests.get(url, headers=auth_header, stream=False)
            if response.status_code == 200:
                break
            else:
                print("[ERROR] TRY:{} Invalid response from url \n error_response{}".format(x, response.text))
        file_name = file_name + '.csv'
        #jsondata = response.json()
        # change the condition according
        if url_part == 'odata':
            jsondata = json.loads(response.text)['value']
        print(jsondata[0])
        # create the directory if not present
        today_dt = format(today.strftime("%F"))
        output_key_today = os.path.join(key_name, today_dt)
        ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
        out = ROOT_DIR + '\output'
        output_dir = os.path.join(out, output_key_today)
        print("[INFO] Checking output dir '{}'".format(output_dir))
        create_directory(output_dir)
        file_path = output_dir
        full_file_path = os.path.join(file_path, file_name)
        data_file = open(full_file_path, 'w', encoding='utf-8', newline='')
        # local_file = os.path.join(ROOT_DIR, opt_dict["global_k"])
        print (opt_dict["global_f"])
        csv_writer = csv.writer(data_file)
        print("[INFO] Writing output file to => ", full_file_path)
        count = 0
        for data in jsondata:
            if count == 0:
                header = data.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(data.values())
        data_file.close()
        upload.upload_file(full_file_path, key_name)