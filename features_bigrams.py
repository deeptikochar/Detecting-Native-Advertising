import boto
from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
import re
import csv
from csv import DictWriter
from bs4 import BeautifulSoup


bucket = 'bds-new'
def process_data(s3keyDict):
    """
    Fetch data with the given s3 key and pass along the contents as a string.

    :param s3key: An s3 key path string.
    :return: A tuple (file_name, data) where data is the contents of the 
        file in a string. Note that if the file is compressed the string will 
        contain the compressed data which will have to be unzipped using the 
        gzip package.
    """
    print "FETCH_DATA gets called"
    words = re.findall('\w+', s3keyDict["content"])
    bigram_counts = {}
    bigram_counts['filename'] = os.path.basename(s3keyDict["filename"])
    bigram_temp = {}
    n = len(words)
    for i in range(0, n-1):
        bigram = words[i] + ' ' + words[i+1]
        if bigram in bigram_temp:
            bigram_temp[bigram] += 1
        else:
            bigram_temp[bigram] = 1  
    for bigram in bigram_temp:
        if bigram_temp[bigram] > 1:
            bigram_counts[bigram] = bigram_temp[bigram]
    return os.path.basename(s3keyDict["filename"]), bigram_counts

conn = boto.connect_s3()
# bucket is the name of the S3 bucket where your data resides

b = conn.get_bucket(bucket, validate=False)
# inkey_root is the S3 'directory' in which your files are located
inkey_root = '3'
keys = b.list(prefix=inkey_root)

key_list = [key.name for key in keys]

key_list.pop(0)

data = []

for s3key in key_list:
    k = b.get_key(s3key)
    dictTemp = {}
    dictTemp["filename"] = s3key
    soup = BeautifulSoup(k.get_contents_as_string())
    soup.get_text().replace("\n",'')
    for script in soup(["script", "style"]):
        script.extract()    # rip it out
    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = '\n'.join(chunk for chunk in chunks if chunk)
    dictTemp["content"] = text
    data.append(dictTemp)

conn.close()

#sc = pyspark.SparkContext('local', 'Whatever')
# Create an RDD from the list of s3 key names to process stored in key_list
file_list = sc.parallelize(data)

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


