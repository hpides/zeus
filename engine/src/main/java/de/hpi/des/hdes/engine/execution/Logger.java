package de.hpi.des.hdes.engine.execution;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.hpi.des.hdes.engine.execution.logdata.AJoinReadData;
import de.hpi.des.hdes.engine.execution.logdata.AJoinTriggerData;
import de.hpi.des.hdes.engine.io.DirectoryHelper;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Logger implements Runnable, Stoppable {

    private boolean running = true;
    private BufferedWriter aJoinReadLog;
    @Getter
    final private ConcurrentLinkedQueue<AJoinReadData> ajoinReadQueue = new ConcurrentLinkedQueue<>();
    private BufferedWriter aJoinTriggerLog;
    @Getter
    final private ConcurrentLinkedQueue<AJoinTriggerData> ajoinTriggerQueue = new ConcurrentLinkedQueue<>();

    public Logger() {
        Date date = Calendar.getInstance().getTime();
        DateFormat dateFormat = new SimpleDateFormat("hh-mm-ss");
        String strDate = dateFormat.format(date);
        try {
            log.info("Log file {} created", DirectoryHelper.getLogPath() + "ajoin_reads_" + strDate + ".csv");
            aJoinReadLog = new BufferedWriter(
                    new FileWriter(DirectoryHelper.getLogPath() + "ajoin_reads_" + strDate + ".csv"));
            aJoinReadLog.write("startTime,diffTime,eventCount,side");
            aJoinReadLog.newLine();
            log.info("Log file {} created", DirectoryHelper.getLogPath() + "ajoin_triggers_" + strDate + ".csv");
            aJoinTriggerLog = new BufferedWriter(
                    new FileWriter(DirectoryHelper.getLogPath() + "ajoin_triggers_" + strDate + ".csv"));
            aJoinTriggerLog.write("startTime,diffTime,eventCount,window");
            aJoinTriggerLog.newLine();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        while (running) {
            while (!ajoinReadQueue.isEmpty()) {
                logAJoinRead(ajoinReadQueue.poll());
            }
            while (!ajoinTriggerQueue.isEmpty()) {
                logAJoinTrigger(ajoinTriggerQueue.poll());
            }
        }

    }

    @Override
    public void shutdown() {
        running = false;
        while (!ajoinReadQueue.isEmpty()) {
            logAJoinRead(ajoinReadQueue.poll());
        }
        while (!ajoinTriggerQueue.isEmpty()) {
            logAJoinTrigger(ajoinTriggerQueue.poll());
        }
        try {
            aJoinReadLog.flush();
            aJoinTriggerLog.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void logAJoinRead(AJoinReadData data) {
        if (data == null) {
            return;
        }
        try {
            aJoinReadLog.write(Long.toString(data.getStartTime()) + "," + Long.toString(data.getDiffTime()) + ","
                    + Integer.toString(data.getEventCount()) + "," + data.getSide());
            aJoinReadLog.newLine();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void logAJoinTrigger(AJoinTriggerData data) {
        if (data == null) {
            return;
        }
        try {
            aJoinTriggerLog.write(Long.toString(data.getStartTime()) + "," + Long.toString(data.getDiffTime()) + ","
                    + Integer.toString(data.getEventCount()) + "," + data.getWindow());
            aJoinTriggerLog.newLine();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}