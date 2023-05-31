"""
This script takes the labels of a specific language as the value, and writes the corresponding qid as the key into lmdb

""" 

import argparse
import multiprocessing
from functools import partial 
import time
import ujson as json
import os
import lmdb
from tqdm import tqdm

manager = multiprocessing.Manager()
def jsonl_generator(fname):
    for line in open(fname, 'r'):
        line = line.strip()
        if len(line) < 3:
            d = {}
        elif line[len(line) - 1] == ',':
            d = json.loads(line[:len(line) - 1])
        else:
            d = json.loads(line)
        yield d

def get_batch_files(fdir):
    filenames = os.listdir(fdir)
    filenames = [os.path.join(fdir, f) for f in filenames]
    return filenames

def get_arg_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', type = str, default = 'wikidata_processed/', help = 'path to output directory')
    parser.add_argument('--la', type = str, default = 'en', help = 'language')
    parser.add_argument('--num_procs', type = int, default=28, help ='Number of processes')
    return parser 


def write_func(filename,la):
    env = lmdb.open("/raid_elmo/home/lr/lyu/wikidata_processed/"+la+"_labels_lmdb", map_size=10737418240)
    txn = env.begin(write=True)
    for json_file in tqdm(filename):
        for item in jsonl_generator(json_file):
            txn.put(key = item['qid'].encode(), value = item['label'].encode())
    txn.commit()
    env.close()
            

def main():
    args = get_arg_parser().parse_args()
    la=get_arg_parser().parse_args().la
    table_files =get_batch_files(args.data+la+'/labels')
    write_func(table_files,la),

    
                
if __name__ == "__main__":
    main()
