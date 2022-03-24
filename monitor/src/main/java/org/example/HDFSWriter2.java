package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HDFSWriter2 implements Closeable {

    private FileSystem fs = null;
    private String currentFile = "13_13_2013.log";
    private FSDataOutputStream outputStream = null;

    /**
     * @param buffer The buffer of message. Note: the buffer might not be a full 1024 messages
     *               if the day ended before 1024 messages have been received.
     * @param date The name of the file in which this buffer should be logged*/
    public boolean write(String[] buffer, String date) throws IOException {
        System.out.println("========== STARTING "+buffer.length);
        Path path = new Path(date);

        if (!currentFile.equals(date)){

            if (outputStream != null)
                outputStream.close();

            currentFile = date;
            try{
                System.out.println("========== CREATING");
                outputStream = fs.create(path, false);
            }
            catch(IOException e){
                System.out.println("========== APPENDING");
                outputStream = fs.append(path);
            }
        }else{
            System.out.println("========== APPENDING");
        }
        System.out.println(outputStream);
        for (String msg : buffer) {
            outputStream.write(msg.getBytes(StandardCharsets.UTF_8)); //write UTF-8
            outputStream.writeChar('\n');
        }
        outputStream.hflush();
        System.out.println("========== FINISHED");
        return true;
    }

    public HDFSWriter2() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop-namenode:9820");
        conf.set("dfs.replication", "1"); //might remove the bad datanode error
        fs = FileSystem.get(conf);
    }

    @Override
    public void close() throws IOException {
        outputStream.close();
    }
}
