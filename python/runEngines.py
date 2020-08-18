import os
import subprocess
from time import sleep
import json
import psutil
from datetime import datetime

prefix = ['cmd.exe', '/c'] if os.name == 'nt' else []
with open('benchmark.json') as f:
    config = json.load(f)

# compileJava = False

# tis = 50
# gh = '127.0.0.1'
# TYPE = 'compiledmaxpric'

# MEASURE_UTILIZATION = False
# UPDATE_RATE = 1

if config['compile_java']:
    c = subprocess.run(prefix + ['mvn', 'package', '-DskipTests'], cwd='../')
    print(c)

def run_engine(add: int, remove: int, batches: int, op: str, initial: int):
    if os.name == 'nt':
        sync_time = subprocess.run(['cmd.exe', '/c', 'w32tm /resync /nowait'])
        print(sync_time)

    path = os.path.normpath(
        'benchmark/target/engine-jar-with-dependencies.jar')
    args = ['java', '-Xms10g', '-Xmx10g', '-jar', path,
            '-gh', config['gh'],
            '-tis', config['tis'],
            '-nqs', add,
            '-rqs', remove,
            '-bat', batches,
            '-t', op,
            '-fq', initial,
            '--basicPort1', config['port1'],
            '--basicPort2', config['port2']]
    args = [str(arg) for arg in args]
    try:
        with subprocess.Popen(args, cwd='../') as proc:
            if config['measure_utilization']:
                py = psutil.Process(proc.pid)
                timestamp = datetime.now()
                save_path = f"../output/utilization_{config['type']}_{timestamp}.csv"
                with open(save_path, "a") as monitor_file:
                    monitor_file.write("cpu_usage, memory_usage\n")
                    while proc.poll() == None:
                        usage_in_gb = round(py.memory_info().rss / 10 ** 9, 2)
                        usage_cpu = py.cpu_percent()
                        monitor_file.write(f"{usage_cpu}, {usage_in_gb}\n")
                        sleep(config['update_rate'])

    except subprocess.TimeoutExpired as e:
        print('timedout', e)

    print('—' * 80)
    print('\n')
    sleep(11)


def run_engine_flink(op: str, initial: int):
    if os.name == 'nt':
        sync_time = subprocess.run(['cmd.exe', '/c', 'w32tm /resync /nowait'])
        print(sync_time)
    gh = '192.168.0.24'

    path = os.path.normpath(
        'flink-benchmark/target/flink-benchmark-1.0-SNAPSHOT.jar')
    args = prefix + ['java', '-Xms10g', '-Xmx10g', '-jar', path,
                     '-gh', gh,
                     '-t', t,
                     '-op', op,
                     '-fq', initial]
    args = [str(arg) for arg in args]
    try:
        c = subprocess.run(args,
                           cwd='../', timeout=config.tis + 1)
        print(c)
    except subprocess.TimeoutExpired as e:
        print('timedout', e)

    print('—' * 80)
    print('\n')
    sleep(12)  # attention flink needs to sleep longer than the generator or
    # else it will crash


def run_add_remove(op: str):
    # Add/Remove
    run_engine(2, 2, 12, op, 1)
    run_engine(4, 4, 12, op, 1)
    run_engine(8, 8, 12, op, 1)

    # Additional high-workload tests
    if op is not "2" and op is not "4" and op is not "5":
        run_engine(10, 10, 12, op, 1)

        run_engine(2, 2, 120, op, 1)
        run_engine(4, 4, 120, op, 1)
        run_engine(8, 8, 120, op, 1)
        run_engine(10, 10, 120, op, 1)

    # Fixed Queries
    run_engine(0, 0, 0, op, 1)
    run_engine(0, 0, 0, op, 10)
    run_engine(0, 0, 0, op, 20)
    run_engine(0, 0, 0, op, 30)

    if op is "1":
        run_engine(0, 0, 0, op, 100)

    # Add over time
    run_engine(1, 0, 12, op, 1)

    # Remove over time
    run_engine(0, 1, 12, op, 31)


def run_flink(op):
    run_engine_flink(op, 1)
    run_engine_flink(op, 10)
    run_engine_flink(op, 20)
    run_engine_flink(op, 30)


# Run Configs
# run_add_remove('nfilter')
# run_flink('1')

# run_add_remove('njoin')
# run_add_remove('najoin')
# run_add_remove('hotcat')
# run_add_remove('maxpric')

run_engine(0, 0, 0, config['type'], 1)

# run_flink('2')
# run_flink('4')
# run_flink('5')
