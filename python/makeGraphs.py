import glob
import os

import matplotlib.pyplot as plt
import pandas as pd


def calculateLatency(timeA, timeB):
  return timeB - timeA


filenameSockets = [file for file in
                   glob.glob(os.path.expanduser('~') + os.sep + 'socket*')]
filenameSinks = [file for file in
                 glob.glob(os.path.expanduser('~') + os.sep + 'sink*')]

df_sockets = []
for socketFile in filenameSockets:
  df_sockets.append(pd.read_csv(socketFile, header=0, sep=","))

# Event Time, Processing Time, Ejection Time
df_sinks = []
for sinkFile in filenameSinks:
  if 'flink' in sinkFile:
    df_sinks.append(pd.read_csv(sinkFile, header=None,
                                names=['eventTime',
                                       'processingTime',
                                       'ejectionTime'
                                       ], sep=",")[:-1])
  else:
    df_sinks.append(pd.read_csv(sinkFile, header=0, sep=",")[:-1])

for df, name in zip(df_sockets, filenameSockets):
  df['average15s'] = df['events'].rolling(15).mean()
  df = df[['events', 'average15s']]
  df.plot(kind="line", title=name)
  plt.show()

for df, name in zip(df_sinks, filenameSinks):
  df['eventTimeLatency'] = df['ejectionTime'] - df['eventTime']
  df['processingTimeLatency'] = df['ejectionTime'] - df['processingTime']
  df['time'] = (df['ejectionTime'] - df['ejectionTime'][0]) / 1_000
  ax = df.plot(kind="line", y=['eventTimeLatency', 'processingTimeLatency'],
               title=name,
               x='time')
  plt.show()
