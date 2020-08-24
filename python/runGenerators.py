import os
import subprocess
from time import sleep
import json
import sys

prefix = ['cmd.exe', '/c'] if os.name == 'nt' else []

with open('benchmark.json') as f:
    config = json.load(f)

# compileJava = False
if config['compile_java']:
    c = subprocess.run(prefix + ['mvn', 'package', '-DskipTests'], cwd='../', )
    print(c)

# eps = 200_000
# tis = 310  # should be ~10 more than the engine
# t = 'new'  # type
# ams = 2
# nr_single_source = 1  # number of queries that need single source
# count = 0

os.makedirs(f"../output/{config['type']}/{sys.argv[1]}", exist_ok=True)
outputPath = f"output/{config['type']}/{sys.argv[1]}"

if os.name == 'nt':
    sync_time = subprocess.run(['cmd.exe', '/c', 'w32tm /resync /nowait'])
    print(sync_time)
config['count'] += 1
if config['count'] > config['nr_single_source']:  # check other file
    config['ams'] = 2
args = prefix + ['numactl', '--physcpubind', config['numa_physical_cpu_generators'], '--interleave', config['numanode_generators'],
            'java', '-jar', os.path.normpath(
            'benchmark/target/generator-jar-with-dependencies.jar'),
            '-eps', config['eps'],
            '-tis', config['tis'] + 10,
            '-bsp1', config['port1'],
            '-bsp2', config['port2'],
            '-t', config['t'],
            '-ams', config['ams'],
            '--outputPath', outputPath]
try:
    c = subprocess.run([str(arg) for arg in args], cwd='../')
    print(c)
except subprocess.TimeoutExpired as e:
    print('timedout', e)
sleep(10)
print('—' * 80)
print('\n')
