package de.hpi.des.hdes.benchmark;

import java.util.ArrayDeque;

public class FileSinkFactory {
    private String prefix;
    private int everyXTuple;
    private ArrayDeque<FileSink<?>> sinks;

    public FileSinkFactory(String prefix, int fixedQueries, int batches, int added, int removed, int everyXTuple){
        this.prefix = prefix.concat("_")
                .concat(Integer.toString(fixedQueries)).concat("_")
                .concat(Integer.toString(batches)).concat("_")
                .concat(Integer.toString(added)).concat("_")
                .concat(Integer.toString(removed));
        this.everyXTuple = everyXTuple;
        sinks = new ArrayDeque<>();
    }

    public int getSinkAmount(){
        return sinks.size();
    }

    public FileSink createFileSink(int index){
        FileSink sink =  new FileSink(Integer.toString(index).concat("_").concat(prefix), everyXTuple);
        sinks.add(sink);
        return sink;
    }

    public FileSink createFileSink(int index, int writeEvery){
        FileSink sink =  new FileSink(Integer.toString(index).concat("_").concat(prefix), everyXTuple);
        sinks.add(sink);
        return sink;
    }

    public void flushSinks(){
        sinks.forEach(FileSink::flush);
        sinks = new ArrayDeque<>();
    }
}
