import json
from collections import Counter
import numpy as np
import pandas as pd
import re
import datetime
from urlparse import urlparse, parse_qs
import httplib
import multiprocessing
from joblib import Parallel, delayed
from tqdm import tqdm
tqdm.monitor_interval = 0

num_cores = 20
print('Total number of cores: {}'.format(num_cores))

def read_file(filename):
    with open(filename,'r') as fr:
        content = fr.read().split('\n')
    return content
 
def getExtendedURL(url):
    extended_url = unshortenURL(url)
    with open('extendedURLs.txt','a+') as fw:
        fw.write(str(url) + '\t' + str(extended_url) + '\n')

def unshortenURL(url):
    try:
        parsed = urlparse(url)

        if parsed.scheme == 'https':
            h = httplib.HTTPSConnection(parsed.netloc)
        else:
            h = httplib.HTTPConnection(parsed.netloc)

        resource = parsed.path
        if parsed.query != "": 
            resource += "?" + parsed.query
        h.request('HEAD', resource )
        response = h.getresponse()
        if response.status/100 == 3 and response.getheader('Location'):
            return unshortenURL(response.getheader('Location')) # changed to process chains of short urls
        else:
            return url 
    except:
        return url
if __name__ == "__main__":
    content = read_file('allURLs.txt')
    content = list(set(content))
    inputs = tqdm(content)
    processed_list = Parallel(n_jobs=num_cores)(delayed(getExtendedURL)(i) for i in inputs)
