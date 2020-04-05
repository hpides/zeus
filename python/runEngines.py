import os
import subprocess
from time import sleep

prefix = ['cmd.exe', '/c'] if os.name == 'nt' else []
compileJava = False

t = "nexmark"
tis = 300
gh = '192.168.0.24'

if compileJava:
    c = subprocess.run(prefix + ['mvn', 'package', '-DskipTests'], cwd='../')
    print(c)


def run_engine(add: int, remove: int, wait: int, op: str, initial: int):
    if os.name == 'nt':
        sync_time = subprocess.run(['cmd.exe', '/c', 'w32tm /resync /nowait'])
        print(sync_time)

    path = os.path.normpath('benchmark/target/engine-jar-with-dependencies.jar')
    args = ['java', '-Xms10g', '-Xmx10g', '-jar', path,
            '-gh', gh,
            '-tis', tis,
            '-t', t,
            '-nqs', add,
            '-rqs', remove,
            '-wbt', wait,
            '-op', op,
            '-fq', initial]
    args = [str(arg) for arg in args]
    try:
        c = subprocess.run(args,
                           cwd='../', timeout=tis + 5)
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
    run_engine(2, 2, 10, op, 1)
    run_engine(4, 4, 10, op, 1)
    run_engine(8, 8, 10, op, 1)

    # Additional high-workload tests
    if op is not "2" and op is not "4" and op is not "5":
        run_engine(10, 10, 10, op, 1)

        run_engine(2, 2, 1, op, 1)
        run_engine(4, 4, 1, op, 1)
        run_engine(8, 8, 1, op, 1)
        run_engine(10, 10, 1, op, 1)

    # Fixed Queries
    run_engine(0, 0, 0, op, 1)
    run_engine(0, 0, 0, op, 10)
    run_engine(0, 0, 0, op, 20)
    run_engine(0, 0, 0, op, 30)

    if op is "1":
        run_engine(0, 0, 0, op, 100)

    # Add over time
    run_engine(1, 0, 10, op, 1)

    # Remove over time
    run_engine(0, 1, 10, op, 31)


def run_flink(op):
    run_engine_flink(op, 1)
    run_engine_flink(op, 10)
    run_engine_flink(op, 20)
    run_engine_flink(op, 30)


# Run Configs
run_add_remove('1')
run_flink('1')

run_add_remove('2')
run_add_remove('3')
run_add_remove('4')
run_add_remove('5')

run_flink('2')
run_flink('4')
run_flink('5')

