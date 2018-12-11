import matplotlib.pyplot as plt
import numpy as np
import math
import re
import sys
import os

root_dir = os.getcwd()
metrics_dir = os.path.join(root_dir, "init_exp_results", "metrics")
graphs_dir = os.path.join(root_dir, "report", "graphs")

total_mem = 32  # single node memory in Ki
num_nodes = 5  # number of machines
num_cores = 12 # number of cores

# terms = ["napoleon", "interpol", "belt", "kind", "idea", "current", "other", "public"]

# TODO : misspelled napoleon for one of the experiments, need to rerun
terms = ["interpol", "belt", "kind", "idea", "current", "other", "public"]

doc_freq = {
    "napoleon": 10522,
    "interpol": 50551,
    "belt": 99287,
    "kind": 506423,
    "idea": 1003549,
    "current": 4897864,
    "other": 9988438,
    "public": 15568449
}

exp_results = {
    'solr': {
        "napoleon": 59345,
        "interpol": 191769,
        "belt": 312432,
        "kind": 1000119,
        "idea": 1625906,
        "current": 3950407,
        "other": 6342624,
        "public": 9462147
    },
    'spark_solr': {
        "napoleon": 18403,
        "interpol": 152145,
        "belt": 182208,
        "kind": 261689,
        "idea": 1720868,
        "current": 9797357,
        "other": 19737940,
        "public": 28968021
    },
    'hdfs_spark': {
        "napoleon": 2254306,
        "interpol": 2256404,
        "belt": 2286729,
        "kind": 2296842,
        "idea": 2402400,
        "current": 2344478,
        "other": 2494579,
        "public": 2513627
    }
}

ps_aux_regex = r"^(\d+)\t(root|j474lee)[ ]+\d+[ ]+(\d+(\.\d*)?)[ ]+(\d+(\.\d*)?)[ ]+(\d+)[ ]+(\d+)"


def get_driver_metrics(exp_type, term):
    file_name = os.path.join(metrics_dir, exp_type + "_" + term + ".txt")
    target_str = "--term " + term
    run_time = exp_results[exp_type][term] / 1000

    cpu_usage = []
    mem_usage = []

    with open(file_name) as f:
        for line in f.readlines():
            if target_str in line:
                match_result = re.match(ps_aux_regex, line)
                if match_result:
                    log_time = int(match_result.group(1))
                    CPU = float(match_result.group(3))  # % cpu
                    MEM = float(match_result.group(5))  # % mem

                    if log_time < run_time:
                        cpu_usage.append(CPU)
                        mem_usage.append(MEM)
                    else:
                        break
                else:
                    print("no match found for driver metrics", line)
                    sys.exit()

    return (run_time, cpu_usage, mem_usage)


pod_regex_format = r"^(\d+)\t([a-z|\d-]+)[ ]+(\d+)m[ ]+(\d+)(Gi|Mi|Ki)[ ]+\snode(\d)"

def get_pod_metrics(exp_type, term, log_length):
    file_name = os.path.join(metrics_dir, exp_type + "_" + term + "_pod.txt")
    run_time = exp_results[exp_type][term] / 1000

    spark_cpu_usage = []
    hdfs_cpu_usage = []
    solr_cpu_usage = []

    spark_mem_usage = []
    hdfs_mem_usage = []
    solr_mem_usage = []

    last_log_time = 0

    spark_cpu_group = []
    hdfs_cpu_group = []
    solr_cpu_group = []

    spark_mem_group = []
    hdfs_mem_group = []
    solr_mem_group = []

    with open(file_name) as f:
        for line in f.readlines():
            match_result = re.match(pod_regex_format, line)
            if match_result:
                log_time = int(match_result.group(1))
                label = match_result.group(2)

                # filter lines according to exp_type
                if "hdfs" not in exp_type and "hdfs" in label: continue
                if "solr" not in exp_type and ("solr" in label or label.startswith("zookeeper")): continue
                if "spark" not in exp_type and "spark" in label: continue

                CPU = int(match_result.group(3))  # milliCPU
                MEM = int(match_result.group(4))  # mem
                MEM_UNIT = match_result.group(5)  # Gi - GB, Mi - MB, Ki - KB
                # node = int(match_result.group(6))  # node num

                if last_log_time == 0 and log_time != 60:
                    # target pod was not started until current log_time
                    spark_cpu_usage += ([0] * int(log_time / 60))
                    hdfs_cpu_usage += ([0] * int(log_time / 60))
                    solr_cpu_usage += ([0] * int(log_time / 60))

                    spark_mem_usage += ([0] * int(log_time / 60))
                    hdfs_mem_usage += ([0] * int(log_time / 60))
                    solr_mem_usage += ([0] * int(log_time / 60))

                if log_time > last_log_time:
                    # flush the last group

                    # TODO: convert to percentage?
                    total_spark_cpu = np.sum(spark_cpu_group)
                    total_hdfs_cpu = np.sum(hdfs_cpu_group)
                    total_solr_cpu = np.sum(solr_cpu_group)
                    total_cpu = total_spark_cpu + total_hdfs_cpu + total_solr_cpu

                    spark_cpu_usage.append(total_spark_cpu / total_cpu * 100)
                    hdfs_cpu_usage.append(total_hdfs_cpu / total_cpu * 100)
                    solr_cpu_usage.append(total_solr_cpu / total_cpu * 100)

                    total_spark_mem = np.sum(spark_mem_group)
                    total_hdfs_mem = np.sum(hdfs_mem_group)
                    total_solr_mem = np.sum(solr_mem_group)
                    total_mem = total_spark_mem + total_hdfs_mem + total_solr_mem

                    spark_mem_usage.append(total_spark_mem / total_mem * 100)
                    hdfs_mem_usage.append(total_hdfs_mem / total_mem * 100)
                    solr_mem_usage.append(total_solr_mem / total_mem * 100)

                    if log_length == len(spark_cpu_usage): break

                    spark_cpu_group = []
                    hdfs_cpu_group = []
                    solr_cpu_group = []

                    spark_mem_group = []
                    hdfs_mem_group = []
                    solr_mem_group = []

                    last_log_time = log_time

                # convert milliCPU to CPU
                CPU /= (1000 * num_cores)

                # convert all memory to Gi
                converted = MEM

                if MEM_UNIT == "Mi":
                    converted /= 1024
                    if MEM_UNIT == "Ki":
                        converted /= 1024

                if "spark" in label:
                    spark_cpu_group.append(CPU)
                    spark_mem_group.append(converted)
                elif "hdfs" in label:
                    hdfs_cpu_group.append(CPU)
                    hdfs_mem_group.append(converted)
                elif "solr" in label:
                    solr_cpu_group.append(CPU)
                    solr_mem_group.append(converted)

            else:
                print("no match found for driver metrics", line)
                sys.exit()

    return (run_time, spark_cpu_usage, hdfs_cpu_usage, solr_cpu_usage, spark_mem_usage, hdfs_mem_usage, solr_mem_usage)

# graph functions

term_exps = {}
term_driver_cpu_totals = {}
term_spark_cpu_totals = {}
term_solr_cpu_totals = {}
term_hdfs_cpu_totals = {}

term_driver_mem_totals = {}
term_spark_mem_totals = {}
term_solr_mem_totals = {}
term_hdfs_mem_totals = {}

def draw_line_exp():
    # plot line graph for each experiment's CPU and memory usage
    # values aggregated for all terms

    for term in terms:
        for exp_type in exp_results.keys():
            total_cpu_usage = term_exps[term][exp_type]['cpu_usage']
            total_solr_cpu_usage = term_exps[term][exp_type]['solr_cpu_usage']
            total_spark_cpu_usage = term_exps[term][exp_type]['spark_cpu_usage']
            total_hdfs_cpu_usage = term_exps[term][exp_type]['hdfs_cpu_usage']

            total_mem_usage = term_exps[term][exp_type]['mem_usage']
            total_solr_mem_usage = term_exps[term][exp_type]['solr_mem_usage']
            total_spark_mem_usage = term_exps[term][exp_type]['spark_mem_usage']
            total_hdfs_mem_usage = term_exps[term][exp_type]['hdfs_mem_usage']

            # cpu
            plt.clf()
            plt.xlabel('Time')
            plt.ylabel('CPU Usage %')
            plt.title("exp: " + exp_type + " - cpu")

            plt.plot(total_cpu_usage)
            legend = [exp_type + ' - Driver CPU Usage']
            if "solr" in exp_type:
                plt.plot(total_solr_cpu_usage)
                legend.append(exp_type + ' - Solr CPU Usage')
            if "spark" in exp_type:
                plt.plot(total_spark_cpu_usage)
                legend.append(exp_type + ' - Spark CPU Usage')
            if "hdfs" in exp_type:
                plt.plot(total_hdfs_cpu_usage)
                legend.append(exp_type + ' - Hdfs CPU Usage')

            plt.legend(legend, loc='upper right')

            plt.savefig(os.path.join(graphs_dir, "cpu_time_" + exp_type + "_" + term + ".png"))

            # memory
            plt.clf()
            plt.xlabel('Time')
            plt.ylabel('Memory Usage %')
            plt.title("exp: " + exp_type + " - mem")

            plt.plot(total_mem_usage)
            legend = [exp_type + ' - Driver Memory Usage']
            if "solr" in exp_type:
                plt.plot(total_solr_mem_usage)
                legend.append(exp_type + ' - Solr Memory Usage')
            if "spark" in exp_type:
                plt.plot(total_spark_mem_usage)
                legend.append(exp_type + ' - Spark Memory Usage')
            if "hdfs" in exp_type:
                plt.plot(total_hdfs_mem_usage)
                legend.append(exp_type + ' - Hdfs Memory Usage')

            plt.legend(legend, loc='upper right')

            plt.savefig(os.path.join(graphs_dir, "mem_time_" + exp_type + "_" + term + ".png"))

def draw_bar_terms():
    # plot bar graph for total cpu and memory usage

    max_driver_cpu_usage_len = max([len(term_exps[term][exp_type]['cpu_usage']) for term in terms for exp_type in exp_results.keys()])

    # pad
    for term in terms:
        for exp_type in exp_results.keys():

            driver_log_length = len(term_exps[term][exp_type]['cpu_usage'])

            if driver_log_length < max_driver_cpu_usage_len:
                # pad remaining indices with zero
                pad_size = max_driver_cpu_usage_len - driver_log_length
                term_exps[term][exp_type]['cpu_usage'] = np.pad(term_exps[term][exp_type]['cpu_usage'], (0, pad_size), 'constant')
                term_exps[term][exp_type]['mem_usage'] = np.pad(term_exps[term][exp_type]['mem_usage'], (0, pad_size), 'constant')

            spark_log_length = len(term_exps[term][exp_type]['spark_cpu_usage'])

            if spark_log_length < max_driver_cpu_usage_len:
                # pad remaining indices with zero
                pad_size = max_driver_cpu_usage_len - spark_log_length
                term_exps[term][exp_type]['spark_cpu_usage'] = np.pad(term_exps[term][exp_type]['spark_cpu_usage'], (0, pad_size), 'constant')
                term_exps[term][exp_type]['spark_mem_usage'] = np.pad(term_exps[term][exp_type]['spark_mem_usage'], (0, pad_size), 'constant')

            solr_log_length = len(term_exps[term][exp_type]['solr_cpu_usage'])

            if solr_log_length < max_driver_cpu_usage_len:
                # center align and pad with first/last value
                pad_size = max_driver_cpu_usage_len - solr_log_length

                left_pad = math.ceil(pad_size/2)
                right_pad = pad_size - left_pad

                term_exps[term][exp_type]['solr_cpu_usage'] = np.pad(term_exps[term][exp_type]['solr_cpu_usage'], (left_pad, right_pad), 'edge')
                term_exps[term][exp_type]['solr_mem_usage'] = np.pad(term_exps[term][exp_type]['solr_mem_usage'], (left_pad, right_pad), 'edge')

            hdfs_log_length = len(term_exps[term][exp_type]['hdfs_cpu_usage'])

            if hdfs_log_length < max_driver_cpu_usage_len:
                # center align and pad with first/last value
                pad_size = max_driver_cpu_usage_len - hdfs_log_length

                left_pad = math.ceil(pad_size/2)
                right_pad = pad_size - left_pad

                term_exps[term][exp_type]['hdfs_cpu_usage'] = np.pad(term_exps[term][exp_type]['hdfs_cpu_usage'], (left_pad, right_pad), 'edge')
                term_exps[term][exp_type]['hdfs_mem_usage'] = np.pad(term_exps[term][exp_type]['hdfs_mem_usage'], (left_pad, right_pad), 'edge')

    # cpu
    exp1_driver_cpu_usage = np.sum([term_exps[term]['solr']['cpu_usage'] for term in terms], axis=1)
    exp1_solr_cpu_usage = np.sum([term_exps[term]['solr']['solr_cpu_usage'] for term in terms], axis=1)

    exp2_driver_cpu_usage = np.sum([term_exps[term]['spark_solr']['cpu_usage'] for term in terms], axis=1)
    exp2_solr_cpu_usage = np.sum([term_exps[term]['spark_solr']['solr_cpu_usage'] for term in terms], axis=1)
    exp2_spark_cpu_usage = np.sum([term_exps[term]['spark_solr']['spark_cpu_usage'] for term in terms], axis=1)

    exp3_driver_cpu_usage = np.sum([term_exps[term]['hdfs_spark']['cpu_usage'] for term in terms], axis=1)
    exp3_spark_cpu_usage = np.sum([term_exps[term]['hdfs_spark']['spark_cpu_usage'] for term in terms], axis=1)
    exp3_hdfs_cpu_usage = np.sum([term_exps[term]['hdfs_spark']['hdfs_cpu_usage'] for term in terms], axis=1)

    ###

    # memory
    exp1_driver_mem_usage = np.sum([term_exps[term]['solr']['mem_usage'] for term in terms], axis=1)
    exp1_solr_mem_usage = np.sum([term_exps[term]['solr']['solr_mem_usage'] for term in terms], axis=1)

    exp2_driver_mem_usage = np.sum([term_exps[term]['spark_solr']['mem_usage'] for term in terms], axis=1)
    exp2_solr_mem_usage = np.sum([term_exps[term]['spark_solr']['solr_mem_usage'] for term in terms], axis=1)
    exp2_spark_mem_usage = np.sum([term_exps[term]['spark_solr']['spark_mem_usage'] for term in terms], axis=1)

    exp3_driver_mem_usage = np.sum([term_exps[term]['hdfs_spark']['mem_usage'] for term in terms], axis=1)
    exp3_spark_mem_usage = np.sum([term_exps[term]['hdfs_spark']['spark_mem_usage'] for term in terms], axis=1)
    exp3_hdfs_mem_usage = np.sum([term_exps[term]['hdfs_spark']['hdfs_mem_usage'] for term in terms], axis=1)

    X = np.arange(len(terms))

    plt.title("Total CPU Usage % vs Selectivity")

    # exp1
    plt1 = plt.bar(X, exp1_driver_cpu_usage, color='b', width=0.25)
    plt2 = plt.bar(X, exp1_solr_cpu_usage, bottom=exp1_driver_cpu_usage, color='y', width=0.25)

    # exp2
    plt.bar(X + 0.30, exp2_driver_cpu_usage, color='g', width=0.25)
    plt.bar(X + 0.30, exp2_solr_cpu_usage, bottom=exp2_driver_cpu_usage, color='y', width=0.25)
    plt.bar(X + 0.30, exp2_spark_cpu_usage, bottom=exp2_driver_cpu_usage + exp2_solr_cpu_usage, color='m', width=0.25)

    # exp3
    plt.bar(X + 0.60, exp3_driver_cpu_usage, color='r', width=0.25)
    plt3 = plt.bar(X + 0.60, exp3_spark_cpu_usage, bottom=exp3_driver_cpu_usage, color='m', width=0.25)
    plt4 = plt.bar(X + 0.60, exp3_hdfs_cpu_usage, bottom=exp3_driver_cpu_usage + exp3_spark_cpu_usage, color='c', width=0.25)

    plt.xticks(X, terms)
    plt.xlabel('Search Term')
    plt.ylabel('Total Driver CPU Usage %')
    plt.legend((plt1[0], plt2[0], plt3[0], plt4[0]), ('Driver %', 'Solr %', 'Spark %', 'HDFS %'))

    plt.savefig(os.path.join(graphs_dir, "cpu_selectivity.png"))

    # memory
    plt.title("Total Mem Usage % vs Selectivity")

    # exp1
    plt1 = plt.bar(X, exp1_driver_mem_usage, color='b', width=0.25)
    plt2 = plt.bar(X, exp1_solr_mem_usage, bottom=exp1_driver_mem_usage, color='y', width=0.25)

    # exp2
    plt.bar(X + 0.30, exp2_driver_mem_usage, color='g', width=0.25)
    plt.bar(X + 0.30, exp2_solr_mem_usage, bottom=exp2_driver_mem_usage, color='y', width=0.25)
    plt.bar(X + 0.30, exp2_spark_mem_usage, bottom=exp2_driver_mem_usage + exp2_solr_mem_usage, color='m', width=0.25)

    # exp3
    plt.bar(X + 0.60, exp3_driver_mem_usage, color='r', width=0.25)
    plt3 = plt.bar(X + 0.60, exp3_spark_mem_usage, bottom=exp3_driver_mem_usage, color='m', width=0.25)
    plt4 = plt.bar(X + 0.60, exp3_hdfs_mem_usage, bottom=exp3_driver_mem_usage + exp3_spark_mem_usage, color='c', width=0.25)

    plt.xticks(X, terms)
    plt.xlabel('Search Term')
    plt.ylabel('Total Driver Memory Usag %')
    plt.legend((plt1[0], plt2[0], plt3[0], plt4[0]), ('Driver %', 'Solr %', 'Spark %', 'HDFS %'))

    plt.savefig(os.path.join(graphs_dir, "mem_selectivity.png"))

def draw_runtime():
    # plot bar graph for driver runtime for each experiment

    exp1_runtime = [term_exps[term]['solr']['runtime'] for term in terms]
    exp2_runtime = [term_exps[term]['spark_solr']['runtime'] for term in terms]
    exp3_runtime = [term_exps[term]['hdfs_spark']['runtime'] for term in terms]

    X = np.arange(len(terms))

    plt.title("Execution Time vs Selectivity")

    # driver totals
    plt.bar(X, exp1_runtime, color='b', width=0.25)

    # solr totals
    plt.bar(X + 0.25, exp2_runtime, color='g', width=0.25)

    # spark totals
    plt.bar(X + 0.50, exp3_runtime, color='r', width=0.25)

    plt.xticks(X, terms)
    plt.xlabel('Search Term')
    plt.ylabel('Execution Time (s)')
    plt.legend(['Solr', 'SolrSpark', 'HdfsSpark'], loc='upper right')

    plt.savefig(os.path.join(graphs_dir, "runtime_selectivity.png"))

###


for term in terms:

    term_exps[term] = {}

    for exp_type in exp_results.keys():

        term_exps[term][exp_type] = {}

        print("processing ", exp_type, " -  term ", term)
        # driver log
        (driver_run_time, cpu_usage, mem_usage) = get_driver_metrics(exp_type, term)

        term_exps[term][exp_type]['runtime'] = driver_run_time

        # to algin driver log with pod log
        driver_log_length = len(cpu_usage)

        term_exps[term][exp_type]['cpu_usage'] = cpu_usage
        term_exps[term][exp_type]['mem_usage'] = mem_usage

        (run_time, spark_cpu_usage, hdfs_cpu_usage, solr_cpu_usage, spark_mem_usage, hdfs_mem_usage, solr_mem_usage) = get_pod_metrics(exp_type, term, driver_log_length)

        term_exps[term][exp_type]['spark_cpu_usage'] = spark_cpu_usage
        term_exps[term][exp_type]['hdfs_cpu_usage'] = hdfs_cpu_usage
        term_exps[term][exp_type]['solr_cpu_usage'] = solr_cpu_usage

        term_exps[term][exp_type]['spark_mem_usage'] = spark_mem_usage
        term_exps[term][exp_type]['hdfs_mem_usage'] = hdfs_mem_usage
        term_exps[term][exp_type]['solr_mem_usage'] = solr_mem_usage

        print('\trun_time - ', driver_run_time, 's')
        print('\tlog length - ', driver_log_length, 'm')


# draw_runtime()
# draw_line_exp()
draw_bar_terms()

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
