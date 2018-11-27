import datetime
import math
import os
import subprocess
import warc.warc
from urlparse import urlsplit

#  python load_clueweb.py

DATA_PATH = '/solr_data/ClueWeb09b'
HDFS_DIR = '/ClueWeb09b_raw'

def exec_cmd(command) :
    print("\t\texecuting - " + str(command))
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    return process

def get_file_name_from_url(url):
    url.replace("https://","")
    url.replace("http://","")
    parsed_url = urlsplit(url)
    return parsed_url.netloc.replace(".", "_") + parsed_url.path.replace("/", "_")


total = 0
for (_, _, filenames) in os.walk(DATA_PATH):
    if len(filenames) != 0:
        total += len(filenames)

print("total # of files = " + str(total))
print("start loading " + str(datetime.datetime.now()))

count = 0

for (dirpath, dirnames, filenames) in os.walk(DATA_PATH):
    hdfs_dirpath = dirpath[len(DATA_PATH):]
    exec_cmd("hadoop fs -mkdir -p " + HDFS_DIR + hdfs_dirpath)

    if len(filenames) != 0:
        print('\tinnermost dir : ' + hdfs_dirpath)
        filenames.sort()
        for filename in filenames:
            folder_name = filename[:-8]
            exec_cmd("hadoop fs -mkdir -p " + HDFS_DIR + hdfs_dirpath + "/" +folder_name )

            if filename.endswith(".gz"):
                file = warc.open(dirpath+"/"+filename)
                for record in file:
                    if record.type == "response":
                        url = record.url
                        file_name = get_file_name_from_url(record.url)

                        with open(file_name,"w") as file:
                            file.write(record.payload)
                            file.close()

                        destination = HDFS_DIR + hdfs_dirpath + "/" +folder_name + "/" + file_name
                        exec_cmd("hdfs dfs -Ddfs.replication=3 -put " + file_name + " " + destination)
                        os.remove(file_name)

            count += 1

        print("current progress " + str(count) + "/" + str(total) + " (" + str(math.round(count/total * 100)) + "%) " + str(datetime.datetime.now()))

print("data load was successful " + str(datetime.datetime.now()))
