#!/bin/env python
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import sys
import json
import subprocess
import uuid
import time

HOME_DIR = os.path.expanduser("~")

def sec2time(s):
    m, s = divmod(s, 60)
    h, m = divmod(m, 60)
    d, h = divmod(h, 24)
    return "%d days %02d:%02d:%02d" % (d, h, m, s)

def s4cmd_run(command, preset, extra_s4_opts=''):
    aws_host, access_key, secret_key = preset['AWS_HOST'], preset['AWS_ACCESS_KEY_ID'], preset['AWS_SECRET_ACCESS_KEY']
    s4cmd_opts = '--endpoint-url=http://{}/ --access-key={} --secret-key={} {}'.format(aws_host, access_key, secret_key, extra_s4_opts) 
    child = subprocess.Popen('S4CMD_OPTS="{}" s4cmd {}'.format(s4cmd_opts, command),shell=True,stdout=subprocess.PIPE)
    streamdata = child.communicate()[0].strip().decode('utf-8')
    if child.returncode != 0:
        raise ValueError('s4cmd command failed')
    return streamdata

def load_s3tos3_config(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config

def ls_stores(config, ls_path='', ls_idx=-1):
    if ls_idx > len(config) - 1:
        raise ValueError('ls_idx should be either -1 or in [0,{}] but got: {}'.format(len(config)-1,ls_idx))
    elif ls_idx >= 0 :
        config = config[ls_idx:ls_idx+1]
    for i,preset in enumerate(config):
        print('=== HOST[idx = {}]: {} ==='.format(i,preset['AWS_HOST']))
        print(s4cmd_run('ls ' + ls_path, preset))

def sync_between_stores(config, src_idx, dest_idx, src_path, dest_path, tmp_dir, dry_run=False, extra_s4_opts=''):
    if src_idx > len(config)-1:
        raise ValueError('There are {} presets, but you specified src_idx={} greater than the number of presets'.format(len(config), src_idx))
    elif dest_idx > len(config)-1:
        raise ValueError('There are {} presets, but you specified dest_idx={} greater than the number of presets'.format(len(config), dest_idx))
    elif not src_path.startswith('s3://'): 
        raise ValueError('src_path should be of the form s3://bucket/blah but found: [{}]'.format(src_path))
    elif not dest_path.startswith('s3://'): 
        raise ValueError('dest_path should be of the form s3://bucket/blah but found: [{}]'.format(dest_path))
    elif src_path.endswith('/') and not dest_path.endswith('/'):
        raise ValueError('If src_path ends with "/" then dest_path must also end with "/"')

    src_preset, dest_preset = config[src_idx], config[dest_idx]
    
    src_is_dir = src_path.endswith('/')
    dest_is_dir = dest_path.endswith('/')
    # Copy from src store to local
    src_files = s4cmd_run(('ls -r ' if src_is_dir else 'ls ') + src_path, src_preset).split('\n') 

    if extra_s4_opts:
        print('=== Using extra S4CMD_OPTS [{}] ==='.format(extra_s4_opts))
    print('=== Copying from [{}] to [{}] ==='.format(src_preset['AWS_HOST'], dest_preset['AWS_HOST']))
    for i,line in enumerate(src_files):
        tokens = line.split()
        dir_or_size, single_src_path = tokens[-2], tokens[-1]
        if dir_or_size == 'DIR' or single_src_path.endswith('/'):
            continue
        rel_path = single_src_path[len(src_path)+1:]
        single_dest_path = os.path.join(dest_path,rel_path) if rel_path else rel_path
        if rel_path:
            single_dest_path = os.path.join(dest_path,rel_path)
        elif dest_is_dir:
            single_dest_path = os.path.join(dest_path,os.path.basename(single_src_path))
        else:
            single_dest_path = dest_path
        if dry_run:
            print('[{}/{}] {} -> {}'.format(i+1, len(src_files), single_src_path, single_dest_path))
        else:
            tmp_filename = os.path.join(tmp_dir, os.path.basename(single_src_path))
            try:
                print('[{}/{}] SRC->LOCAL->DEST | {} -> {}'.format(i+1, len(src_files), single_src_path, tmp_filename), end='')
                start = time.time()
                s4cmd_run('get -s {} {}'.format(single_src_path, tmp_filename), src_preset, extra_s4_opts=extra_s4_opts)
                print(' -> {}'.format(single_dest_path), end='')
                s4cmd_run('put -s {} {}'.format(tmp_filename, single_dest_path), dest_preset, extra_s4_opts=extra_s4_opts)
                print(' | Took [{}]'.format(sec2time(time.time()-start)))
            except:
                os.unlink(tmp_filename)
                sys.stderr.write('Something went wrong, exitting and cleaning up')
                sys.exit(1)
            os.unlink(tmp_filename)
    

if __name__ == "__main__":
    example_usage = '''\
Example usage:
    
    Using this script requires a json config in ~/.s3tos3.config with lists of storages, here's an example:

[
  {
    "AWS_HOST": "host1",
    "AWS_ACCESS_KEY_ID": "XXXXXXXXXXXXXXXXXXXX",
    "AWS_SECRET_ACCESS_KEY": "YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY"
  },
  {
    "AWS_HOST": "host2",
    "AWS_ACCESS_KEY_ID": "ZZZZZZZZZZZZZZZZZZZZ",
    "AWS_SECRET_ACCESS_KEY": "WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW"
  }
]

    # Lists all buckets in all object stores listed in config
    python s3tos3.py --ls_all 

    # Dry run of the sync. src_idx and dest_idx refer to the index of the object store within the config
    # This will copy s3://root/file.txt -> s3://workspace/file.txt
    python s3tos3.py --src_idx 0 --dest_idx 1 --src_path s3://root/file.txt --dest_path s3://workspace/ --dry_run
    python s3tos3.py --src_idx 0 --dest_idx 1 --src_path s3://root/file.txt --dest_path s3://workspace/file.txt --dry_run

    # Dry run of the sync. src_idx and dest_idx refer to the index of the object store within the config
    # This will copy s3://root/* -> s3://workspace/*
    python s3tos3.py --src_idx 0 --dest_idx 1 --src_path s3://root/file.txt --dest_path s3://workspace/ --dry_run
    python s3tos3.py --src_idx 0 --dest_idx 1 --src_path s3://root/ --dest_path s3://workspace/ --dry_run

    # You can also pass forward args to s4cmd. Any arg that this script does not consume (with the exception of --dry_run) 
    #  are passed straight to s4cmd
    python s3tos3.py --src_idx 0 --dest_idx 1 --src_path s3://root/ --dest_path s3://workspace/ --multipart-split-size=100000000 -c 8 -t 3

'''
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description=example_usage)
    parser.add_argument('--ls_all', default=False, action='store_true', help='Will call ls at the root level of all object stores (default: %(default)r)')
    parser.add_argument('--ls_idx', type=int, default=None, help='s4cmd ls a particular object ls_idx=-1 means all stores (default: %(default)r)')
    parser.add_argument('--ls_path', type=str, default='', help='Path to ls for a particular object store, used with --ls_idx (default: %(default)r)')
    parser.add_argument('--s3tos3_config', type=str, default=os.path.join(HOME_DIR,'.s3tos3.config'), required=False, help='Config for s3tos3 (default: %(default)r)')
    parser.add_argument('--src_path', type=str, default='', help='Source path (default: %(default)r)')
    parser.add_argument('--dest_path', type=str, default='', help='Destination path (default: %(default)r)')
    parser.add_argument('--src_idx', type=int, default=None, help='Source object store index (default: %(default)r)')
    parser.add_argument('--dest_idx', type=int, default=None, help='Destination object store index (default: %(default)r)')
    parser.add_argument('--tmp_dir', type=str, default=os.path.join(os.sep,'tmp'), help='The local tmp dir to copy from src to dest (default: %(default)r)')
    parser.add_argument('-n','--dry_run', default=False, action='store_true', help='Dryrun sync (default: %(default)r)')

    known_args, remaining_args = parser.parse_known_args()
    if known_args.dry_run:
        remaining_args.append('--dry-run')

    if not os.path.exists(known_args.s3tos3_config):
        raise ValueError('s3tos3 config {} does not exist'.format(known_args.s3tos3_config))

    config = load_s3tos3_config(known_args.s3tos3_config)

    if known_args.ls_all and known_args.ls_idx is not None:
        raise ValueError('Should not provide both ls_all and ls_idx')
    elif known_args.ls_all or known_args.ls_idx is not None:
        ls_stores(config, known_args.ls_path, known_args.ls_idx if known_args.ls_idx is not None else -1)
        sys.exit(0)
    
    sync_between_stores(config,
                        known_args.src_idx,
                        known_args.dest_idx,
                        known_args.src_path,
                        known_args.dest_path,
                        known_args.tmp_dir,
                        known_args.dry_run,
                        extra_s4_opts=' '.join(remaining_args)
                        )


