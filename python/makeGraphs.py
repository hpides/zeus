import datetime
import math
import os

import glob
import matplotlib.pyplot as plt
import pandas as pd

# Instructions at the bottom of the script

if False:
    import matplotlib

    matplotlib.rcParams['interactive'] == True
    matplotlib.use('MacOSX')

save = True
only_agg = True
fsize = (8, 6)
plot_name = "DEFAULT"
comp_with_flink = False
no_title = True
sort_sinks = True


def extractInfo(sink_name: str, socket=''):
    prefix = 'sock ' + socket.split('socket')[1].split('_')[0] if socket else ''
    if 'flink' in sink_name:
        return prefix + ' flink ' + sink_name.split('_')[3] + ' f: ' + \
               sink_name.split('%')[1]

    type = sink_name.split('_')[2]
    param_list = sink_name.split('%')[1:8]
    additions = param_list[0]
    removals = param_list[2]
    wait = param_list[4]
    fixed = param_list[6]
    if comp_with_flink:
        return prefix + ' ' + type + ' f:' + fixed
    else:
        return prefix + ' ' + type + ' a:' + additions + ' r:' + removals + ' ' \
                                                                            'w:' + wait + ' f:' + fixed


def make_plot_name(prefix, newline=False):
    if no_title:
        return ""
    return prefix + ' ' + ' '.join(n for n in ' '.join(plot_name.split(
        'V')).split(' ') if not n.isdecimal()) + '\n' if newline else ''

def extractInfoWithNumber(sink_name: str):
    base = extractInfo(sink_name)
    return 'sink ' + sink_name.split('_')[1] + base


def saveIfNeeded(plt, name, skip=False):
    if save and not skip:
        plt.savefig('chart_' + name + '.pdf', format='pdf', bbox_inches='tight')


def should_take_sink(name: str):
    should = False
    for element in must_contain:
        if element in name:
            should = True
    for element in must_not_contain:
        if element in name:
            should = False
    return should


def read_mulitple_dir(directories: list):
    l_df_sinks = []
    l_df_sockets = []
    l_matches = []
    l_filenameSinks = []
    l_filenameSockets = []
    for dire in directories:
        a_df_sinks, a_df_sockets, a_matches, a_filenameSinks, a_filenameSockets = \
            read_and_match_files(dire)
        l_df_sinks.extend(a_df_sinks)
        l_df_sockets.extend(a_df_sockets)
        l_matches.extend(a_matches)
        l_filenameSinks.extend(a_filenameSinks)
        l_filenameSockets.extend(a_filenameSockets)
    return l_df_sinks, l_df_sockets, l_matches, l_filenameSinks, l_filenameSockets


def read_and_match_files(directory: str):
    run_dir = r"~/hdes-data/" + directory
    filenameSockets = sorted([file for file in glob.glob(
        os.path.expanduser(run_dir) + os.sep + 'socket*')])
    filenameSinks = sorted([file for file in glob.glob(
        os.path.expanduser(run_dir) + os.sep + 'sink*')])

    matches = []
    for sourceName in filenameSockets:
        sourceTime = list(map(int, sourceName.split('_')[-1][0:-4].split('-')))
        sourceTime = datetime.datetime(2020, 1, 1, sourceTime[0], sourceTime[
            1], sourceTime[2]).timestamp()
        candidate = filenameSinks[0]
        candidateTime = list(
            map(int, candidate.split('_')[-1][1:-4].split('-')))
        candidateTime = datetime.datetime(2020, 1, 1, candidateTime[0],
                                          candidateTime[
                                              1], candidateTime[2]).timestamp()
        for name in filenameSinks[1:]:
            time = list(map(int, name.split('_')[-1][1:-4].split('-')))
            time = datetime.datetime(2020, 1, 1, time[0], time[
                1], time[2]).timestamp()
            if math.fabs(sourceTime - time) < math.fabs(
                sourceTime - candidateTime):
                candidateTime = time
                candidate = name
        matches.append((candidate, sourceName))

    print('Sockets and Matches:', matches)

    df_sockets = []
    remaining_matches = []
    for sinkFile, socketFile in matches:
        if should_take_sink(sinkFile):
            try:
                df_sockets.append(
                    pd.read_csv(socketFile, header=0, sep=",", dtype={
                        'seconds': 'Int64', 'events': 'Int64'}))
            except Exception as e:
                matches = [m for m in matches if socketFile not in m]
                print('Skipped ', socketFile)
                print(e)
            remaining_matches.append((sinkFile, socketFile))
    matches = remaining_matches
    # Event Time, Processing Time, Ejection Time
    df_sinks = []
    existingSinks = []
    for sinkFile in filenameSinks:
        if should_take_sink(sinkFile):
            try:
                if 'flink' in sinkFile:
                    flink_df = pd.read_csv(sinkFile, header=None,
                                           names=['eventTime',
                                                  'processingTime',
                                                  'ejectionTime'
                                                  ], sep=",", dtype={
                            'eventTime': 'Int64', 'processingTime': 'Int64',
                            'ejectionTime': 'Int64'})[:-1]
                    if flink_df.shape[0] > 0:
                        df_sinks.append(flink_df)
                    else:
                        raise Exception("empty csv")

                else:
                    df_sinks.append(
                        pd.read_csv(sinkFile, header=0, sep=",", dtype={
                            'eventTime': 'Int64', 'processingTime': 'Int64',
                            'ejectionTime': 'Int64'})[:-1])
                existingSinks.append(sinkFile)
            except Exception as e:
                print('Skipped ', sinkFile)
                print(e)
    filenameSinks = existingSinks


    matches = [extractInfo(m, sock) for m, sock in matches]
    print(matches)
    return df_sinks, df_sockets, matches, filenameSinks, filenameSockets


# Ingested Tuples sum
def ingested_tuples_sum():
    sum = {'name': [], 'events': []}
    for df, name, match in zip(df_sockets, filenameSockets, matches):
        sum['name'].append(match)
        sum['events'].append(df['events'].sum())
    sumdf = pd.DataFrame(sum)
    sumdf.plot(kind='bar', x='name', y='events',
               title=make_plot_name('#Ingested Tuples'))
    plt.ylabel('#events')
    saveIfNeeded(plt, 'IngestedTuples')
    plt.show()


def ingested_tuples_sum_collapsed_bar():
    sums = {}
    for df, name, match in zip(df_sockets, filenameSockets, matches):
        if sums.get(match[6:]) is not None:
            new_df = sums[match[6:]] + df['events']
            sums[match[6:]] = new_df
        else:
            sums[match[6:]] = df['events']

    sumdf = pd.DataFrame.from_dict(sums)
    sumdf.plot(kind='bar',
               title=make_plot_name('#Ingested Tuples'))
    plt.ylabel('Events per second')
    plt.title(make_plot_name('Events per second'))
    saveIfNeeded(plt, 'IngestedTuplesBar' + plot_name)
    plt.show()


# Ingested Tuples sum
def ingested_tuples_sum_collapsed_box():
    sums = {}
    for df, name, match in zip(df_sockets, filenameSockets, matches):
        if sums.get(match[6:]) is not None:
            new_df = sums[match[6:]] + df['events']
            sums[match[6:]] = new_df
        else:
            sums[match[6:]] = df['events']

    sumdf = pd.DataFrame.from_dict(sums)
    sumdf.boxplot(rot=90, showfliers=False)
    plt.ylabel('Events per second')
    plt.title(make_plot_name('Events per second', True))
    saveIfNeeded(plt, 'IngestedTuples' + plot_name)
    plt.show()


# Ejected Tuples sum
def ejected_tuples_sum():
    sum = {'name': [], 'events': []}
    for df, name in zip(df_sinks, filenameSinks):
        sum['name'].append(extractInfoWithNumber(name))
        sum['events'].append(df['ejectionTime'].count() * 1000)
    sumdf = pd.DataFrame(sum)
    sumdf.plot(kind='bar', x='name', y='events', title='#Resulting Tuples')
    plt.ylabel('avg_events/sec')
    saveIfNeeded(plt, 'resultingTuples' + plot_name)
    plt.show()




# Comparison average ingested tuples
def avg_ingested_tup_comp():
    ax = None
    for df, name, match in zip(df_sockets, filenameSockets, matches):
        appendix = match
        df['avg15 ' + appendix] = df['events'].rolling(15).mean()
        df[appendix] = df['events']
        df = df[[appendix, 'avg15 ' + appendix]]
        ax = df.plot(kind="line", title="Throughput", ax=ax, figsize=fsize)
    plt.ylabel('#events')
    plt.xlabel('seconds')
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.10), ncol=3)
    saveIfNeeded(plt, 'ThroughputComp' + plot_name)
    plt.show()


def throughput_diagramm():
    ax = None
    temp = {}
    for df, name, match in zip(df_sockets, filenameSockets, matches):
        appendix = match[7:]
        df[appendix] = df['events']
        df = df[[appendix]][:301]
        if temp.get(appendix) is not None:
            combined_df = temp.get(appendix) + df
            ax = combined_df.plot(kind="line", title=make_plot_name(
                "Throughput"),
                                  ax=ax,
                                  figsize=fsize)
        elif 'map' in appendix or 'filter' in appendix or 'Filter' in appendix:
            ax = df.plot(kind="line", title=make_plot_name("Throughput"), ax=ax,
                         figsize=fsize)
        else:
            temp[appendix] = df
    plt.ylabel('#events')
    plt.xlabel('seconds')
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.10), ncol=3)
    saveIfNeeded(plt, 'ThroughputComp' + plot_name)
    plt.show()


def throughput_rolling_average(rolling_avg):
    ax = None
    temp = {}
    for df, name, match in zip(df_sockets, filenameSockets, matches):
        appendix = match[7:]
        df['avg15 ' + appendix] = df['events'].rolling(rolling_avg).mean()
        df = df[['avg15 ' + appendix]][:301]
        if temp.get(appendix) is not None:
            combined_df = temp.get(appendix) + df
            ax = combined_df.plot(kind="line", title=make_plot_name(
                "Throughput"),
                                  ax=ax,
                                  figsize=fsize)
        else:
            temp[appendix] = df
    plt.ylabel('#events')
    plt.xlabel('seconds')
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.10), ncol=3)
    saveIfNeeded(plt, 'ThroughputComp' + plot_name)
    plt.show()



# Latency comparision between runs
def latency_comp(etl=True, ptl=True):
    ax = None
    if sort_sinks:
        for sink in df_sinks:
            if ptl:
                sink.sort_values(['processingTime'], inplace=True)
                sink['time'] = (sink['processingTime'] - sink['processingTime'][
                    0]) / 1_000
            if etl:
                sink.sort_values(['eventTime'], inplace=True)
                sink['time'] = (sink['eventTime'] - sink['eventTime'][
                    0]) / 1_000
    for df, name in zip(df_sinks, filenameSinks):
        df['etl'] = df['ejectionTime'] - df['eventTime']
        df['ptl'] = df['ejectionTime'] - df['processingTime']
        # df['time'] = (df['ejectionTime'] - df['ejectionTime'][0]) / 1_000
        y = ['etl'] if etl else []
        y = y + ['ptl'] if ptl else y
        label = [l + extractInfo(name) for l in y]
        df = df[df['time'] < 301]
        df = df[df['time'] >= 0]
        ax = df.plot(kind="line",
                     y=y,
                     label=label,
                     title=make_plot_name('Latency'),
                     x='time', ax=ax, figsize=fsize)
    plt.ylabel('latency in milliseconds')
    plt.xlabel('runtime in seconds')
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.10), ncol=2)

    saveIfNeeded(plt, 'Latency' + plot_name)
    plt.show()


def latency_comp_box():
    final_df = pd.DataFrame()
    for df, name in zip(df_sinks, filenameSinks):
        df['eventTimeLatency'] = df['ejectionTime'] - df['eventTime']
        df['processingTimeLatency'] = df['ejectionTime'] - df['processingTime']
        final_df['etl' + extractInfo(name)] = df['eventTimeLatency']
        final_df['ptl' + extractInfo(name)] = df[
            'processingTimeLatency']

    final_df.boxplot(rot=90, showfliers=False)

    plt.ylabel('Latency in milliseconds')
    plt.title(make_plot_name('Latencies', True))
    saveIfNeeded(plt, 'PLatency-Box' + plot_name)
    plt.show()


def latency_comp_box_etl():
    final_df = pd.DataFrame()
    for df, name in zip(df_sinks, filenameSinks):
        df['eventTimeLatency'] = df['ejectionTime'] - df['eventTime']
        final_df['etl' + extractInfo(name)] = df['eventTimeLatency']

    final_df.boxplot(rot=90, showfliers=False)

    plt.ylabel('Latency in milliseconds')
    plt.title(make_plot_name('Event-Time Latencies', True))
    saveIfNeeded(plt, 'ELatency-Box' + plot_name)
    plt.show()


def latency_comp_box_ptl():
    final_df = pd.DataFrame()
    for df, name in zip(df_sinks, filenameSinks):
        df['processingTimeLatency'] = df['ejectionTime'] - df['processingTime']
        final_df['ptl' + extractInfo(name)] = df[
            'processingTimeLatency']

    final_df.boxplot(rot=90, showfliers=False)

    plt.ylabel('Latency in milliseconds')
    plt.title(make_plot_name('Processing Time Latencies', True))
    saveIfNeeded(plt, 'Latency-Box' + plot_name)
    plt.show()


dirs = ['Basic-AJoin', 'Basic-Join', 'Basic-Map', 'NX-AJoin', 'NX-PJoin',
        'NX-Filter', 'NX-HotCat-PJoin', 'NX-HPPA-AJoin',
        'NX-HPPA-PJoin' 'Basic-Flink-Join']

selected = []
must_contain = []
must_not_contain = []

if not only_agg:
    latency_per_run()
    ingested_per_run()


def flink_hdes_op_comp(op_selected):
    global plot_name, selected, must_contain, must_not_contain, df_sinks, \
        df_sockets, matches, filenameSinks, filenameSockets, comp_with_flink
    comp_with_flink = True
    selected = op_selected
    must_contain = ['f%1', 'f%10', 'f%20', 'f%30']
    must_not_contain = ['e%10', 'e%1']
    plot_name = ' vs. '.join(selected)
    df_sinks, df_sockets, matches, filenameSinks, filenameSockets = \
        read_mulitple_dir(selected)
    ingested_tuples_sum_collapsed_box()
    latency_comp_box_ptl()
    latency_comp_box_etl()
    throughput_diagramm()
    throughput_rolling_average(20)
    must_contain = ['f%1']
    must_not_contain = ['e%10', 'e%1', 'f%10']
    plot_name = ' vs. '.join(selected)
    df_sinks, df_sockets, matches, filenameSinks, filenameSockets = \
        read_mulitple_dir(selected)
    throughput_diagramm()
    latency_comp(etl=False)
    latency_comp(ptl=False)



def add_remove_plots(op_selected):
    global plot_name, selected, must_contain, must_not_contain, df_sinks, \
        df_sockets, matches, filenameSinks, filenameSockets, comp_with_flink
    comp_with_flink = False
    selected = op_selected
    must_contain = ['']
    must_not_contain = ['a%0%_r%0%', 'a%0%_r%1%', 'a%1%_r%0%']
    plot_name = ' vs. '.join(selected)
    df_sinks, df_sockets, matches, filenameSinks, filenameSockets = \
        read_mulitple_dir(selected)
    ingested_tuples_sum_collapsed_box()
    latency_comp_box_ptl()
    latency_comp_box_etl()
    throughput_diagramm()
    latency_comp(etl=False)
    latency_comp(ptl=False)
    must_contain = ['a%0%_r%1%', 'a%1%_r%0%']
    must_not_contain = ['a%0%_r%0%']
    plot_name = ' vs. '.join(selected)
    df_sinks, df_sockets, matches, filenameSinks, filenameSockets = \
        read_mulitple_dir(selected)
    throughput_diagramm()
    latency_comp()

# Add the directories which you want to plot as parameters. They should be in
# the hdes-data directory which should be in the root folder.
# The folders must contain both sink and socket files

# flink_hdes_op_comp(['Basic-AJoin', 'Basic-Join', 'Basic-Flink-Join'])

# add_remove_plots(['NX-AJoin', 'NX-Join'])
