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
import argparse
tqdm.monitor_interval = 0

parser = argparse.ArgumentParser()
parser.add_argument('--input_file', help='Link to input URL file (Each line one URL)')
parser.add_argument('--out_file', help='Link to output URL file')
parser.add_argument('--num_cores', help='Enter number of cores to use')
args = parser.parse_args()

num_cores = int(args.num_cores)


def read_file(filename):
    '''
    Read from input URL file and return the list
    '''
    with open(filename,'r') as fr:
        content = fr.read().split('\n')
    return content
 
def getExtendedURL(url):
    '''
    Get extended URL of each shorten URL and save in output file 
    Output Format: <shortenURL> <expandedURL>
    '''
    extended_url = unshortenURL(url, count=0)
    with open(args.out_file,'a+') as fw:
        fw.write(str(url) + '\t' + str(extended_url) + '\n')

def unshortenURL(url, count):
    try:
        # Try thrice else return the original URL
        if count == 2:
            return url
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
        
        # Increment the value of count by 1
        count = count + 1
        
        if response.status/100 == 3 and response.getheader('Location'):
            return unshortenURL(response.getheader('Location'), count) # changed to process chains of short urls
        else:
            return url 
    except:
        return url
if __name__ == "__main__":
    # Get the URL list from the input file
    content = read_file(args.input_file)
    content = list(set(content))

    # Do some fancy stuff with tqdm
    inputs = tqdm(content)

    # Parallel execution of extracting unshorten URLs
    processed_list = Parallel(n_jobs=num_cores)(delayed(getExtendedURL)(i) for i in inputs)
