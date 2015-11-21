import boto
from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
import re
import csv
from csv import DictWriter
from bs4 import BeautifulSoup
import nltk


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
    b = conn.get_bucket(bucket, validate=False))
    k = b.get_key(s3key)
    data = k.get_contents_as_string()
    conn.close()
    soup = BeautifulSoup(data)
    soup.get_text().replace("\n",'')
    for script in soup(["script", "style"]):
        script.extract()    # rip it out
    text = soup.get_text()
    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    words = re.findall('\w+', text)
    trigram_list = list(nltk.ngrams(words,3))
    trigram_counts = {}
    trigram_counts['filename'] = os.path.basename(s3key)
    num_distinct_trigrams = 0
    for trigram in trigram_list:
        if trigram in trigram_counts:
            trigram_counts[trigram] += 1
        else:
            trigram_counts[trigram] = 1
            num_distinct_trigrams +=1
    trigram_counts['total_trigrams'] = len(trigram_list)
    trigram_counts['distinct_trigrams'] = num_distinct_trigrams   
    return os.path.basename(s3key), trigram_counts

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


