import os
import subprocess
from time import sleep

prefix = ['cmd.exe', '/c'] if os.name == 'nt' else []
compileJava = False
if compileJava:
  c = subprocess.run(prefix + ['mvn', 'package', '-DskipTests'], cwd='../', )
  print(c)
eps = 10_000_000
tis = 310  # should be ~10 more than the engine
t = 'nexmark'  # type
ams = 1
nr_single_source = 0  # number of queries that need single source
count = 0

while True:
  if os.name == 'nt':
    sync_time = subprocess.run(['cmd.exe', '/c', 'w32tm /resync /nowait'])
    print(sync_time)
  count += 1
  if count > nr_single_source:  # check other file
    ams = 2
  args = prefix + ['java', '-jar', os.path.normpath(
      'benchmark/target/generator-jar-with-dependencies.jar'),
                   '-eps', eps,
                   '-tis', tis,
                   '-t', t,
                   '-ams', ams]
  try:
    c = subprocess.run([str(arg) for arg in args], cwd='../', timeout=tis + 5)
    print(c)
  except subprocess.TimeoutExpired as e:
    print('timedout', e)
  sleep(10)
  print('—' * 80)
  print('\n')
