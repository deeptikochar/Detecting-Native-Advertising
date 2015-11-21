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
    conn = boto.connect_s3()
    b = conn.get_bucket(bucket, validate=False)
    k = b.get_key(s3key)
    data = k.get_contents_as_string()
    myregex = r'(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}'
    urls = re.findall(myregex, data)
    domains = []
    domain_counts = {}
    domain_counts['filename'] = os.path.basename(s3key)
    for url in urls:
        domain = url.replace('www.', '')
        domains.append(domain)
    num_distinct_domains = 0
    for domain in domains:
        if domain in domain_counts:
            domain_counts[domain] += 1
        else:
            domain_counts[domain] = 1
            num_distinct_domains +=1
    domain_counts['total_urls'] = len(urls)
    domain_counts['distinct_domains'] = num_distinct_domains
    conn.close()
    return os.path.basename(s3key), domain_counts

conn = boto.connect_s3()
# bucket is the name of the S3 bucket where your data resides

b = conn.get_bucket(bucket, validate=False))  
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

fieldnames = set([])
for i in range(1, len(p), 2):
    fieldnames = fieldnames.union(set(p[i].keys()))

fieldnames = list(fieldnames)
with open('3-formatted.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    headers = {}
    for field in fieldnames:
        headers[field] = field
    writer.writerow(headers)
    for i in range(0, len(p)):
        if i%2 != 0:
            dom = p[i]
            for field in fieldnames:
                if field not in dom:
                    dom[field] = 0
            writer.writerow(dom)


