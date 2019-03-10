# s3tos3
This is a simple utility function that helps sync files or directories between s3 object stores.

Sometimes I would come across a situation where there were two clusters with s3 object stores (not necessarily on the cloud) and I wanted to move content between them. [s4cmd](https://github.com/bloomreach/s4cmd) is a great utility for accessing a single object store, so I wanted this tool to build upon s4cmd.

Prereqs
===
+ python3
+ `pip install s4cmd`

Usage
===
```
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
python s3tos3.py --src_idx 0 --dest_idx 1 --src_path s3://root/ --dest_path s3://workspace/ --dry_run

# You can also pass forward args to s4cmd. Any arg that this script does not consume (with the exception of --dry_run) 
#  are passed straight to s4cmd
python s3tos3.py --src_idx 0 --dest_idx 1 --src_path s3://root/ --dest_path s3://workspace/ --multipart-split-size=100000000 -c 8 -t 3
```

TODOs
===
+ Right now the script will copy one file to disk at a time and then sync that file to the destination object store. Might want to add an option to do these in parallel.
+ Even better, avoid copying to local disk :)
