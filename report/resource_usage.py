import matplotlib.pyplot as plt
import numpy as np
import math
import re
import sys

metrics_dir = "../init_exp_results/metrics/"

# terms = ["napoleon", "interpol", "belt", "kind", "idea", "current", "other", "public"]

# TODO : misspelled napoleon for one of the experiments, need to rerun
terms = ["interpol", "belt", "kind", "idea", "current", "other", "public"]

doc_freq = {
	"napoleon" : 10522, 
	"interpol" : 50551, 
	"belt" : 99287, 
	"kind" : 506423, 
	"idea" : 1003549, 
	"current" : 4897864, 
	"other" : 9988438, 
	"public" : 15568449	
}

exp_results = {
	'seq' : {
		"napoleon" : 59345, 
		"interpol" : 191769, 
		"belt" : 312432, 
		"kind" : 1000119, 
		"idea" : 1625906, 
		"current" : 3950407, 
		"other" : 6342624, 
		"public" : 9462147
	},
	'solr' : {
		"napoleon" : 18403, 
		"interpol" : 152145, 
		"belt" : 182208, 
		"kind" : 261689, 
		"idea" : 1720868, 
		"current" : 9797357, 
		"other" : 19737940, 
		"public" : 28968021
	},
	'spark' : {
		"napoleon" : 2254306, 
		"interpol" : 2256404, 
		"belt" : 2286729, 
		"kind" : 2296842, 
		"idea" : 2402400, 
		"current" : 2344478, 
		"other" : 2494579, 
		"public" : 2513627
	}
}

ps_aux_regex = r"^(\d+)\t(root|j474lee)[ ]+\d+[ ]+(\d+(\.\d*)?)[ ]+(\d+(\.\d*)?)[ ]+(\d+)[ ]+(\d+)"

def get_driver_metrics(exp_type, term):
	file_name = metrics_dir+exp_type + "_" + term + ".txt"
	target_str = "--term " + term
	run_time = exp_results[exp_type][term] / 1000

	cpu_usage = []
	mem_usage = []
	vsz_usage = []
	rss_usage = []

	with open(file_name) as f:
		for line in f.readlines():
			if target_str in line:
				match_result = re.match(ps_aux_regex, line)
				if match_result:
					log_time = int(match_result.group(1))
					# TODO : convert to match pod CPU usage
					CPU = float(match_result.group(3)) # % cpu
					MEM = float(match_result.group(5)) # % mem
					# TODO : confirm that unit match with pod memory logs
					VSZ = int(match_result.group(7)) # virtual size in kilobytes
					RSS = int(match_result.group(8)) # real memory size or resident set size in 1024 byte units

					if log_time < run_time:
						cpu_usage.append(CPU)
						mem_usage.append(MEM)
						vsz_usage.append(VSZ)
						rss_usage.append(RSS)
					else:
						break
				else:
					print("no match found for driver metrics", line)
					sys.exit()

	return (run_time, cpu_usage, mem_usage, vsz_usage, rss_usage)


pod_regex_format = r"^(\d+)\t([a-z|\d-]+)[ ]+(\d+)m[ ]+(\d+)(Gi|Mi|Ki)[ ]+\snode(\d)"

def get_pod_metrics(exp_type, term, log_length, pod_name_filter):
	file_name = metrics_dir+exp_type + "_" + term + "_pod.txt"
	run_time = exp_results[exp_type][term] / 1000

	cpu_usage = [] 
	mem_usage = []

	last_log_time = 0
	cpu_group = []
	mem_group = []

	with open(file_name) as f:
		for line in f.readlines():
			if pod_name_filter in line:
				match_result = re.match(pod_regex_format, line)
				if match_result:
					log_time = int(match_result.group(1))
					# TODO : convert to match driver CPU usage
					CPU = int(match_result.group(3)) # milliCPU
					MEM = int(match_result.group(4)) # mem
					MEM_UNIT = match_result.group(5) # Gi - GB, Mi - MB, Ki - KB
					node = int(match_result.group(6)) # virtual size in kilobytes

					if last_log_time == 0 and log_time != 60:
						# target pod was not started until current log_time 
						cpu_usage += ([0] * int(log_time/60))
						mem_usage += ([0] * int(log_time/60))

					if log_time > last_log_time:
						# flush the last group
						cpu_usage.append(np.sum(cpu_group))
						mem_usage.append(np.sum(mem_group))

						if log_length == len(cpu_usage):
							break

						cpu_group = []
						mem_group = []

						last_log_time = log_time

					cpu_group.append(CPU)

					# TODO : convert as needed to match with driver metrics
					# converted = 1024 * MEM # default Ki (KB)
					converted = MEM

					if MEM_UNIT != "Ki":
						converted *= 1024
						if MEM_UNIT != "Mi":
							converted *= 1024

					mem_group.append(converted)

				else:
					print("no match found for driver metrics", line)
					sys.exit()

	return (run_time, cpu_usage, mem_usage)


for exp_type in exp_results.keys():
	for term in terms:
		print("processing ", exp_type, " -  term ", term)
		# driver log
		(run_time, cpu_usage, mem_usage, vsz_usage, rss_usage) = get_driver_metrics(exp_type, term)
		
		# to algin driver log with pod log
		driver_log_length = len(cpu_usage)

		(run_time, spark_cpu_usage, spark_mem_usage) = get_pod_metrics(exp_type, term, driver_log_length, "spark")

		spark_log_length = len(spark_cpu_usage)

		if spark_log_length < driver_log_length:
			# pad remaining indices with zero
			pad_size = driver_log_length - spark_log_length
			spark_cpu_usage = np.pad(spark_cpu_usage, (0, pad_size), 'constant')
			spark_mem_usage = np.pad(spark_mem_usage, (0, pad_size), 'constant')

			spark_log_length = len(spark_cpu_usage)

		# pods log
		(run_time, solr_cpu_usage, solr_mem_usage) = get_pod_metrics(exp_type, term, driver_log_length, "solr")

		solr_log_length = len(solr_cpu_usage)

		if solr_log_length < driver_log_length:
			# center align and pad with first/last value
			pad_size = driver_log_length - solr_log_length

			left_pad = math.ceil(pad_size/2)
			right_pad = pad_size - left_pad

			solr_cpu_usage = np.pad(solr_cpu_usage, (left_pad, right_pad), 'edge')
			solr_mem_usage = np.pad(solr_mem_usage, (left_pad, right_pad), 'edge')

			solr_log_length = len(solr_cpu_usage)

		print('\trun_time - ', run_time, 's')
		print('\tlog length - ', driver_log_length, 'm')

		# TODO : generate a fiture






# mapping = {}

# graph_freq = 20
# x = []
# y = []

# with open("dist.txt") as f:
#     for line in f.readlines():
#         splits = line.split("\t")
#         freq = int(splits[0])
#         count = int(splits[1])
#         if count > 150 and freq > 10:
#             x.append(freq)
#             y.append(count)

# delta = math.floor(len(x) / graph_freq)

# x = x[0::delta]
# y = y[0::delta]

# plt.subplots_adjust(left=0.2)
# plt.plot(x, y)
# plt.xlabel('Document frequency', fontsize='large')
# plt.ylabel('Number of words', fontsize='large')
# plt.savefig("Document_distribution.pdf")
# plt.show()