import sys
import getopt,re,requests
from typing import Pattern
from urllib.parse import urljoin
import settings

try:
    opts, args = getopt.getopt(sys.argv[1:], 'i:f:t:m:h', ['input=', 'filter=','top=','market=','help'])
except getopt.GetoptError:
    sys.exit(2)

opt_dict = {
    'inp_array': [],
    'global_f': None,
    'global_m': None,
    'global_t': None,
    'global_k': None
}

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
        opt_dict['global_m']  = arg

    else:
        sys.exit(2)

def get_auth_token() :
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

    #try the request 3 times
    for x in range(1,4):
        auth_response = requests.post(auth_url, data=data)
        if auth_response.status_code == 200 :
            break
        else:
            print("[ERROR] TRY:{} Invalid response from url auth_url \n error_response{}",format(x,auth_response.text))

    #token not found exit
    if auth_response.status_code != 200 :
        print("[ERROR] exiting")
        sys.exit(2)

    # Read token from auth response
    auth_response_json = auth_response.json()
    print(auth_response_json)
    auth_token = auth_response_json["access_token"]

    auth_token_header_value = "Bearer %s" % auth_token
    auth_token_header = {"Authorization": auth_token_header_value}
    print("Auth Token obtained successfully")
    return auth_token_header


BASE_URL= "https://<SOME_URL>/api/AAS"

def is_valid_key_rule(url_conf) :
    pattern = {
        'ActASBL':re.compile("\d{4}P\d{2}"),
    }
    key = url_cnf.get('k')
    pattern = pattern.get(key)
    fltr = url_conf.get('f')

    if fltr != None and pattern != None:
        if pattern.match(fltr):
            return 1
        else :
            return 0
    else :
        return 1


auth_header = get_auth_token()
out_url_conf=[]
for inp in opt_dict['inp_array'] :
# k=K1:f=F1:t=T1:m=M1
# k=K2:f=F1
    for split_inp in inp.split(','):
          # k=K1 # f=F1 # t=T1 # m=M1
        url_conf = { 'k': None, 'f': None, 'm': None, 't': None }
        for url_key in url_conf :
            for field in split_inp.split(':'):
                # k, K1 # f,F1 # t,T1 # m,M1
                inp_key, inp_val = field.split('=')
                if inp_key == url_key:
                    url_conf[url_key] = inp_val
                elif inp_key != url_key and url_conf.get(url_key) is None:
                    #set default one if given
                    url_conf[url_key] = opt_dict.get('global_'+url_key)
        #append url conf to array
        out_url_conf.append(url_conf)

# headers = {
#   'Content-Type': 'application/json',
#   'Cookie': 'asid=7558a92b63853c979da96ddaa027af12d9d9bd47'
# }

for url_cnf in out_url_conf:
    if is_valid_key_rule(url_cnf):
        # url = "{BASE_URL}/{key}/{filter}/{market}/{top}".format(BASE_URL=BASE_URL,)
        url = BASE_URL+ '/' + url_cnf.get('k')
        if url_cnf.get('f'):
            url=url + '/' + url_cnf.get('f')
            if url_cnf.get('m'):
                url=url + '/' + url_cnf.get('m')
                if url_cnf.get('t'):
                    url=url + '/' + url_cnf.get('t')
    #empty variable
    response = None
    for x in range(1,4):
        response = requests.request("GET", url, headers=auth_header)
        if response.status_code == 200 :
            break
        else:
            print("[ERROR] TRY:{} Invalid response from url {} \n error_response{}",format(x,url,response.text))

    print(response.json())