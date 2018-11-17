import sys
import os
import subprocess

if len(sys.argv) < 2:
    print("usage: python3 load_data.py <hadoop client pod name>")
    sys.exit()

POD_NAME = sys.argv[1]
DATA_PATH = '/hdd2/collections/'
CLIENT_DATA_PATH = '/solr_data/'

print("hadoop client pod name: ", POD_NAME)
print("data path: ", DATA_PATH)
print("hdfs client data path: ", CLIENT_DATA_PATH, '\n')

def wrap_kubectl_exec(command) :
    return "kubectl exec " + POD_NAME + " -- " + command

def wrap_kubectl_cp(source, dest) :
    return "kubectl cp " + source + " "+ POD_NAME + ":" + dest

def exec_cmd(command) :
    print("\t \t executing - ", command)
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
    process.wait()
    return process

total = 0
for (_, _, filenames) in os.walk(DATA_PATH):
    if len(filenames) != 0:
        total += len(filenames)

print("total # of files = ", total)

count = 0

for (dirpath, dirnames, filenames) in os.walk(DATA_PATH):
    hdfs_dirpath = dirpath[len(DATA_PATH):]

    process = exec_cmd(wrap_kubectl_exec("hadoop fs -mkdir -p /" + hdfs_dirpath))

    if len(filenames) != 0:
        print('\tinnermost dir : ' + hdfs_dirpath)
        filenames.sort()
        for filename in filenames:
            path = hdfs_dirpath + '/' + filename[:-3]

            # generate temp gz
            command = "cp " + CLIENT_DATA_PATH + path + ".gz " + CLIENT_DATA_PATH + path + "_temp.gz"
            exec_cmd(wrap_kubectl_exec(command))

            # decompress
            command = "gunzip " + CLIENT_DATA_PATH + path + ".gz"
            exec_cmd(wrap_kubectl_exec(command))

            # upload to hdfs
            command = "hdfs dfs -Ddfs.replication=3 -put " + CLIENT_DATA_PATH + path + " /" + hdfs_dirpath
            exec_cmd(wrap_kubectl_exec(command))

            # delete decompressed one
            command = "rm -rf " + CLIENT_DATA_PATH + path
            exec_cmd(wrap_kubectl_exec(command))

            # generate original gz from temp gz
            command = "mv " + CLIENT_DATA_PATH + path + "_temp.gz " + CLIENT_DATA_PATH + path + ".gz"
            exec_cmd(wrap_kubectl_exec(command))

            count += 1;

        print("current progress ", count, "/", total, "( ", count/total * 100 ," )")

print("data load was successful")
