import os
import subprocess
from time import sleep
import json
import sys

prefix = ['cmd.exe', '/c'] if os.name == 'nt' else []

path = f"./benchmarks/{sys.argv[2]}.json"
with open(path) as f:
    config = json.load(f)

if config['compile_java']:
    c = subprocess.run(prefix + ['mvn', 'package', '-DskipTests'], cwd='../', )
    print(c)

os.makedirs(f"../output/{config['type']}/{sys.argv[1]}", exist_ok=True)
outputPath = f"output/{config['type']}/{sys.argv[1]}"

if os.name == 'nt':
    sync_time = subprocess.run(['cmd.exe', '/c', 'w32tm /resync /nowait'])
    print(sync_time)

number_generator = 1
if config['type'] == 'report_parallel_ajoin':
    number_generator = config['shared_queries']

for x in range(number_generator):
    args = prefix + ['numactl', '--physcpubind', config['numa_physical_cpu_generators'], '--interleave', config['numanode_generators'],
                'java', '-jar', os.path.normpath(
                'benchmark/target/generator-jar-with-dependencies.jar'),
                '-eps', config['eps'],
                '-tis', config['tis'] + 10,
                '-bsp1', (config['port1'] + x * 2),
                '-bsp2', (config['port2'] + x * 2),
                '-t', config['t'],
                '-ams', config['ams'],
                '--outputPath', outputPath]
    try:
        proc = subprocess.Popen([str(arg) for arg in args], cwd='../')
        print(proc)
    except subprocess.TimeoutExpired as e:
        print('timedout', e)
sleep(config['tis'] + 10)
sleep(15)
print('—' * 80)
print('\n')
