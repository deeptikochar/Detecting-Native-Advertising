import boto
from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
import re
import csv
from csv import DictWriter


bucket = 'bds-new'
def process_data(s3key):
    """
    Fetch data with the given s3 key and pass along the contents as a string.

    :param s3key: An s3 key path string.
    :return: A tuple (file_name, data) where data is the contents of the 
        file in a string. Note that if the file is compressed the string will 
        contain the compressed data which will have to be unzipped using the 
        gzip package.
    """
    print "FETCH_DATA gets called"
    conn = boto.connect_s3()
    b = conn.get_bucket(bucket)
    k = b.get_key(s3key)
    data = k.get_contents_as_string()
    values = {}
    values['filename'] = os.path.basename(s3key)
    values["word_count"] = len(re.split('\s+', data))
    values['line_count'] = data.count('\n')
    values['space_count'] = data.count(' ')
    values['tab_count'] = data.count('\t')
    values['brace_count'] = data.count('{')
    values['bracket_count'] = data.count('(')
    values['length'] = len(data)
    conn.close()
    return os.path.basename(s3key), values





conn = boto.connect_s3()
# bucket is the name of the S3 bucket where your data resides

b = conn.get_bucket(bucket)  
# inkey_root is the S3 'directory' in which your files are located
inkey_root = '3'
keys = b.list(prefix=inkey_root)

key_list = [key.name for key in keys]

key_list.pop(0)

conn.close()

#sc = pyspark.SparkContext('local', 'Whatever')
# Create an RDD from the list of s3 key names to process stored in key_list
file_list = sc.parallelize(key_list)

p = file_list.flatMap(process_data).collect()

with open('final.csv', 'w') as csvfile:
    fieldnames = ['filename', 'word_count', 'line_count', 'space_count', 'tab_count', 'brace_count', 'bracket_count', 'length']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    headers = {}
    for field in fieldnames:
        headers[field] = field
    writer.writerow(headers)
    for i in range(0, len(p)):
        if i%2 != 0:
            writer.writerow(p[i])


