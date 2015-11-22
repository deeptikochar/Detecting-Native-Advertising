import boto
from pyspark import SparkContext, SparkConf
from boto.s3.connection import S3Connection
import re
import csv
from csv import DictWriter
from bs4 import BeautifulSoup


bucket = 'bds-new'
def process_data(s3keyDict):
    print "FETCH_DATA gets called"
    
    words = re.findall('\w+', s3keyDict["content"])
    bag_of_words = {}
    bag_of_words['filename'] = os.path.basename(s3keyDict["filename"])
    num_distinct_words = 0
    for word in words:
        if word in bag_of_words:
            bag_of_words[word] += 1
        else:
            bag_of_words[word] = 1
            num_distinct_words +=1
    bag_of_words['total_words'] = len(words)
    bag_of_words['distinct_words'] = num_distinct_words    
    return os.path.basename(s3keyDict["filename"]), bag_of_words

conn = boto.connect_s3()

b = conn.get_bucket(bucket, validate=False)  
inkey_root = 'temp'
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

file_list = sc.parallelize(data)

p = file_list.flatMap(process_data).collect()

fieldnames = set([])
for i in range(1, len(p), 2):
    fieldnames = fieldnames.union(set(p[i].keys()))

fieldnames = list(fieldnames)
with open('temp-formatted.csv', 'w') as csvfile:
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
