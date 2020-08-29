import os
import subprocess
from time import sleep
import json
import psutil
from datetime import datetime
import sys

prefix = ['cmd.exe', '/c'] if os.name == 'nt' else []
path = f"./benchmarks/{sys.argv[2]}.json"
with open(path) as f:
    config = json.load(f)

if config['compile_java']:
    c = subprocess.run(prefix + ['mvn', 'package', '-DskipTests'], cwd='../')
    print(c)

os.makedirs(f"../output/{config['type']}/{sys.argv[1]}", exist_ok=True)
outputPath = f"output/{config['type']}/{sys.argv[1]}"

def run_engine():
    if os.name == 'nt':
        sync_time = subprocess.run(['cmd.exe', '/c', 'w32tm /resync /nowait'])
        print(sync_time)

    path = os.path.normpath(
        'benchmark/target/engine-jar-with-dependencies.jar')
    memoryMaxFlag = '-Xmx9g'
    memoryMinFlag = '-Xms9g'
    if config['t'] == 'basic':
        memoryMaxFlag = '-Xmx12g'
        memoryMinFlag = '-Xms12g'
    args = ['numactl', '--physcpubind', config['numa_physical_cpu_engine'], '--interleave', config['numanode_engine'],
            'java', memoryMinFlag, memoryMaxFlag, '-jar', path,
            '-gh', config['gh'],
            '-tis', config['tis'],
            '-bsp1', config['port1'],
            '-bsp2', config['port2'],
            '-nsq', config['shared_queries'],
            '-t', config['type'],
            '--outputPath', outputPath]
    
    if 'logging' in config and config['logging']:
        args.append('--logging')
    args = [str(arg) for arg in args]
    try:
        with subprocess.Popen(args, cwd='../') as proc:
            if config['measure_utilization']:
                py = psutil.Process(proc.pid)
                timestamp = datetime.now()
                save_path = f"../{outputPath}utilization_{config['type']}_{timestamp}.csv"
                with open(save_path, "a") as monitor_file:
                    monitor_file.write("cpu_usage, memory_usage\n")
                    while proc.poll() == None:
                        usage_in_gb = round(py.memory_info().rss / 10 ** 9, 2)
                        usage_cpu = py.cpu_percent()
                        monitor_file.write(f"{usage_cpu}, {usage_in_gb}\n")
                        sleep(config['update_rate'])

    except subprocess.TimeoutExpired as e:
        print('timedout', e)

    sleep(10)
    print('—' * 80)
    print('\n')


def run_engine_flink(op: str, initial: int):
    if os.name == 'nt':
        sync_time = subprocess.run(['cmd.exe', '/c', 'w32tm /resync /nowait'])
        print(sync_time)

    path = os.path.normpath(
        'flink-benchmark/target/flink-benchmark-1.0-SNAPSHOT.jar')
    args = prefix + ['numactl', '--physcpubind', config['numa_physical_cpu_engine'], '--interleave', config['numanode_engine'],
                     'java', '-Xms12g', '-Xmx12g', '-jar', path,
                     '-gh', config['gh'],
                     '-op', op,
                     '-bsp1', config['port1'],
                     '-bsp2', config['port2'],
                     '-fq', initial,
                     '--outputPath', outputPath]
    args = [str(arg) for arg in args]
    try:
        c = subprocess.run(args,
                           cwd='../', timeout=config['tis'] + 30)
        print(c)
    except subprocess.TimeoutExpired as e:
        print('timedout', e)

    print('—' * 80)
    print('\n')
    sleep(12)  # attention flink needs to sleep longer than the generator or
    # else it will crash

if config['type'] == 'flink':
    run_engine_flink('join', 1)
else:
    run_engine()
