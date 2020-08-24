package de.hpi.des.hdes.benchmark;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import lombok.Setter;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class BlockingSocket<E> implements BlockingOffer<E> {

    private final AbstractSerializer<E> serializer;
    private OutputStreamWriter outputWriter;
    private BufferedOutputStream outputStream;
    private Socket socket;
    private volatile int[] eventsPerSecond;
    private long startTime;
    private int lastSecond = 0;
    private BufferedWriter measurements;
    private long engineRuntime;
    private int eventsPerSecondCount;
    private volatile boolean wroteFile = false;
    private ServerSocket ss;
    @Setter
    private boolean byteFlag = false;
    @Setter
    private int byteLength;

    BlockingSocket(int port, AbstractSerializer<E> serializer, String filePath, long engineRuntime,
            int eventsPerSecondCount) {
        this.serializer = serializer;
        this.engineRuntime = engineRuntime;
        this.eventsPerSecond = new int[(int) engineRuntime + 300];
        this.eventsPerSecondCount = eventsPerSecondCount;
        Date date = Calendar.getInstance().getTime();
        DateFormat dateFormat = new SimpleDateFormat("hh-mm-ss");
        String strDate = dateFormat.format(date);
        try {
            log.info("Listening on port {}", port);
            ss = new ServerSocket(port);
            File file = new File(filePath.replace(".csv", "_") + strDate + ".csv");
            if (file.createNewFile()) {
                this.measurements = new BufferedWriter(new FileWriter(file));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void waitForConnection() {
        try { // wait until client appears
            this.socket = ss.accept();
            if (byteFlag) {
                this.outputStream = new BufferedOutputStream(this.socket.getOutputStream());
            } else {
                this.outputWriter = new OutputStreamWriter(this.socket.getOutputStream(), StandardCharsets.UTF_8);
            }
            this.startTime = System.nanoTime();

        } catch (ConnectException ignore) {

        } catch (IOException e) {
            e.printStackTrace();
        }
        log.info("connected successfully");
    }

    @Override
    public void offer(E event) {
        try {
            int currentSecond = (int) ((System.nanoTime() - this.startTime) / 1_000_000_000);
            if (this.eventsPerSecond[currentSecond] < eventsPerSecondCount) {
                if (byteFlag) {
                    this.outputStream.write((byte[]) event, 0, byteLength);
                } else {
                    this.outputWriter.write(this.serializer.serialize(event).concat("\n"));
                }
                this.eventsPerSecond[currentSecond] += 1;
            }
            if (currentSecond > lastSecond) {
                log.info(this.eventsPerSecond[lastSecond] + " events for second " + lastSecond);
                this.lastSecond = currentSecond;
            }
        } catch (IOException e) {
            this.writeFile();
            System.exit(1);
            throw new IllegalStateException("Could not offer new data, Engine is shut down");
        }
    }

    public void writeFile() {
        if (!wroteFile) {
            this.wroteFile = true;
            try {
                measurements.write("seconds,events\n");
                for (int i = 0; i < this.engineRuntime; i++) {
                    measurements.write(i + "," + this.eventsPerSecond[i] + "\n");
                }
                measurements.flush();
            } catch (IOException e2) {
                e2.printStackTrace();
            }
            log.info("Wrote file");
        }
        try {
            ss.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void flush() {
        try {
            if (byteFlag) {
                this.outputStream.flush();
            } else {
                this.outputWriter.flush();
            }
        } catch (IOException e) {
            log.warn("Unsuccessful flush, socket already closed by engine");
        }
    }
}
