package de.hpi.des.hdes.benchmark;

import de.hpi.des.hdes.engine.AData;
import de.hpi.des.hdes.engine.io.DirectoryHelper;
import de.hpi.des.hdes.engine.operation.Sink;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class FileSink<E> implements Sink<E> {
    BufferedWriter out;
    int writeEveryX;
    int writtenTuples = 0;

    public FileSink(String name, int writeEveryXTuple) {
        var filePath = DirectoryHelper.getOutputPath() + "sink_" + name + ".csv";
        System.out.println(filePath.toString());
        this.writeEveryX = writeEveryXTuple;
        try {
            Date date = Calendar.getInstance().getTime();
            DateFormat dateFormat = new SimpleDateFormat("hh-mm-ss");
            String strDate = dateFormat.format(date);

            File file = new File(filePath.replace(".csv", "_t") + strDate + ".csv");
            if (file.createNewFile()) {
                this.out = new BufferedWriter(new FileWriter(file), 10_000_000);
                this.out.write("eventTime,processingTime,ejectionTime\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(AData<E> in) {
        try {
            if(this.writtenTuples % writeEveryX == 0) {
                out.write(
                    in.getValue().toString().replace(" ", "").replace("(", "").replace(")", ""));
                out.newLine();
                this.writtenTuples = 0;
            }
            this.writtenTuples += 1;
        } catch (IOException e) {
            this.flush();
            e.printStackTrace();
        }
    }

    public void flush() {
        try {
            this.out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

