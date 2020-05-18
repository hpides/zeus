import os
import subprocess
from time import sleep

prefix = ['cmd.exe', '/c'] if os.name == 'nt' else []
compileJava = False

tis = 120
gh = '127.0.0.1'

if compileJava:
    c = subprocess.run(prefix + ['mvn', 'package', '-DskipTests'], cwd='../')
    print(c)


def run_engine(add: int, remove: int, batches: int, op: str, initial: int):
    if os.name == 'nt':
        sync_time = subprocess.run(['cmd.exe', '/c', 'w32tm /resync /nowait'])
        print(sync_time)

    path = os.path.normpath('benchmark/target/engine-jar-with-dependencies.jar')
    args = ['java', '-Xms10g', '-Xmx10g', '-jar', path,
            '-gh', gh,
            '-tis', tis,
            '-nqs', add,
            '-rqs', remove,
            '-bat', batches,
            '-t', op,
            '-fq', initial]
    args = [str(arg) for arg in args]
    try:
        c = subprocess.run(args,
                           cwd='../')
        print(c)
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
                           cwd='../', timeout=tis + 1)
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
#run_add_remove('nfilter')
#run_flink('1')

#run_add_remove('njoin')
#run_add_remove('najoin')
#run_add_remove('hotcat')
#run_add_remove('maxpric')

run_engine(0, 0, 0, 'najoin', 1)

#run_flink('2')
#run_flink('4')
#run_flink('5')
