# To use this script look at the bottom

import sys, threading, subprocess, os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

path = os.path.join(os.getcwd(), "output")
# Threading

class Engine (threading.Thread):
  def __init__(self, kind, time, fixed, buffer, serializer, batches, added, removed, ports):
    threading.Thread.__init__(self)
    self.cmd = ["java","-Xms10g", "-Xmx10g", "-jar", "benchmark/target/engine-jar-with-dependencies.jar"]
    self.kind = ["--type", kind]
    self.cmd.extend(self.kind)
    self.timeout = time+10
    self.time = ["--timeInSeconds", str(time)]
    self.cmd.extend(self.time)
    self.buffer = ["--networkBufferSize", str(buffer)]
    self.cmd.extend(self.buffer)
    self.fixed = ["--fixedQueries", str(fixed)]
    self.cmd.extend(self.fixed)
    self.serializer = ["--serializer", serializer]
    self.cmd.extend(self.serializer)
    self.batches = ["--batchesAmount", str(batches)]
    self.cmd.extend(self.batches)
    self.added = ["--newQueriesPerBatch", str(added)]
    self.cmd.extend(self.added)
    self.removed = ["--removeQueriesPerBatch", str(removed)]
    self.cmd.extend(self.removed)
    self.host = ["--generatorHost", "127.0.0.1"]
    self.cmd.extend(self.host)
    self.cmd.extend(ports)
  
  def run(self):
    try:
      c = subprocess.run(self.cmd, timeout=self.timeout)
      print(c)
    except subprocess.TimeoutExpired:
      print('Engine Timed Out')

class Generator (threading.Thread):
  def __init__(self, kind, events, time, serializer, ports):
    threading.Thread.__init__(self)
    self.cmd = ["java","-Xms10g", "-Xmx10g", "-jar", "benchmark/target/generator-jar-with-dependencies.jar"]
    self.kind = ["--type", kind]
    self.cmd.extend(self.kind)
    self.timeout = time+10
    self.time = ["--timeInSeconds", str(time)]
    self.cmd.extend(self.time)
    self.events = ["--eventsPerSecond", str(events)]
    self.cmd.extend(self.events)
    self.serializer = ["--serializer", serializer]
    self.cmd.extend(self.serializer)
    self.sources = ["--amountOfSources", str(int(len(ports)/2))]
    self.cmd.extend(self.sources)
    self.cmd.extend(ports)
  
  def run(self):
    try:
      c = subprocess.run(self.cmd, timeout=self.timeout)
      print(c)
    except subprocess.TimeoutExpired:
      print('Generator Timed Out')

def executeJava(kind, events, time, buffer, serializer, fixed, batches, added, removed, sources, datatype):  
  ports = []
  if(datatype == "basic"):
    if(sources == 1):
      ports = ["--basicPort1", "7000"]
    else:
      ports =["--basicPort1", "7000", "--basicPort2", "7001"]
  else:
    if(sources == 1):
      ports = ["--auctionSourcePort", "7000"]
    else:
      ports =["--auctionSourcePort", "7000", "--bidSourcePort", "7001"]

  generator = Generator(
    datatype,
    events,
    time,
    serializer,
    ports
  )
  engine = Engine(
    kind,
    time+5,
    fixed,
    buffer,
    serializer,
    batches,
    added,
    removed,
    ports
  )

  generator.start()
  engine.start()

  return [engine, generator]

def calc():
  data = []
  files = os.listdir(path)
  for file in files:
    if(file.startswith("sink") and file.endswith(".csv")):
      data.append({
        "name": file.split("_")[2],
        "latency": pd.read_csv(os.path.join(path, file)),
        "events": pd.read_csv(os.path.join(path, [f for f in files if f.endswith(file.split("t")[-1])and f.startswith("socket")][0]))
      })
  events = {}
  eventTimeLatency = {}
  processingTimeLatency = {}
  for f in data:
    e = f["events"]["events"]
    events[f["name"]] = [
      np.mean(e),
      np.min(e),
      np.max(e),
      np.percentile(e, 0.25),
      np.percentile(e, 0.75),
    ]
    f["latency"]["eventTime"] = f["latency"]["eventTime"]/1000000
    l = f["latency"]["eventTimeLatency"] = f["latency"]["ejectionTime"]-f["latency"]["eventTime"]
    eventTimeLatency[f["name"]] = [
      np.mean(l),
      np.min(l),
      np.max(l),
      np.percentile(l, 0.25),
      np.percentile(l, 0.75),
    ]
    p = f["latency"]["processingTimeLatency"] = f["latency"]["ejectionTime"]-f["latency"]["processingTime"]
    processingTimeLatency[f["name"]] = [
      np.mean(p),
      np.min(p),
      np.max(p),
      np.percentile(p, 0.25),
      np.percentile(p, 0.75),
    ]
  print("Throughput")
  print(pd.DataFrame(events, index=["Mean", "Min", "Max", "25%", "75%"]))
  fig = plt.figure()
  for d in data:
    plt.plot(d["events"]["events"], label=d["name"], ls="-.")
  plt.ylabel("Events per second")
  plt.legend()
  plt.savefig("throughput.png", bbox_inches="tight", pad_inches=0.3)
  plt.figure()
  plt.boxplot([d["events"]["events"] for d in data], labels=[d["name"] for d in data])
  plt.ylabel("Events per second")
  plt.savefig("throughput-boxplot.png", bbox_inches="tight", pad_inches=0.3)
  print("Event Time Latency")
  print(pd.DataFrame(eventTimeLatency, index=["Mean", "Min", "Max", "25%", "75%"]))
  plt.figure()
  for d in data:
    plt.plot(d["latency"]["eventTimeLatency"], label=d["name"], ls="-.")
  plt.ylabel("Time in ms")
  plt.legend()
  plt.savefig("eventTimeLatency.png", bbox_inches="tight", pad_inches=0.3)
  plt.figure()
  plt.boxplot([d["latency"]["eventTimeLatency"] for d in data], labels=[d["name"] for d in data])
  plt.ylabel("Time in ms")
  plt.savefig("eventTimeLatency-boxplot.png", bbox_inches="tight", pad_inches=0.3)
  print("Processing Time Latency")
  print(pd.DataFrame(processingTimeLatency, index=["Mean", "Min", "Max", "25%", "75%"]))
  plt.figure()
  for d in data:
    plt.plot(d["latency"]["processingTimeLatency"], label=d["name"], ls="-.")
  plt.ylabel("Time in ms")
  plt.legend()
  plt.savefig("processingTimeLatency.png", bbox_inches="tight", pad_inches=0.3)
  plt.figure()
  plt.boxplot([d["latency"]["processingTimeLatency"] for d in data], labels=[d["name"] for d in data])
  plt.ylabel("Time in ms")
  plt.savefig("processingTime-boxplot.png", bbox_inches="tight", pad_inches=0.3)


def executeTest(kind, size, time, buffer, queries, dynamic, sources, datatype):
  runtime =  30 if time == "s" else 60 if time == "m" else 120 if time == "l" else 90
  buffersize =10 if buffer == "s" else 20 if buffer == "m" else 30 if buffer == "l" else 15
  eventamount = 50000 if size == "s" else 100000 if size == "m" else 200000 if size == "l" else 75000
  threads = executeJava(
    kind,
    eventamount,
    runtime,
    buffersize,
    "custom",
    queries,
    int(runtime/10) if dynamic else 0,
    queries if dynamic else 0,
    queries if dynamic else 0,
    sources,
    datatype
  )
  for t in threads:
    t.join()

# to execute a Test use the execute Test method with the following params
# kind: what will get executed as test in java e.g. bfilter, hotcat etc
# size: how many events shall be dispatched per second: either "s","m" or "l"
# time: how long shall the test run: either "s","m" or "l"
# buffer: how big shall the networkbuffer be: either "s","m" or "l"
# queries: how many queries to add: any integer
# dynamic: if queries should be added during runtime: boolean
# sources: how many sources are needed: 1 or 2
# datatype: which data should be generated: either "nexmark" or "basic"

calc()
