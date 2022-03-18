package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class HDFSWriter {

    private FileSystem fs = null;
    /**
     * @param buffer The buffer of message. Note: the buffer might not be a full 1024 messages
     *               if the day ended before 1024 messages have been received.
     * @param date The name of the file in which this buffer should be logged*/
    public boolean write(String[] buffer, String date) throws IOException {
        Path path = new Path(date);

        /* THIS IS NOT DUPLICATE CODE. I've wirtten it this way to be able to use
         * try with resources. */
        if (fs.exists(path)) {
            System.out.println("========== APPENDING");
            try(FSDataOutputStream outputStream = fs.append(path)){
                for (String msg : buffer) {
                    outputStream.write(msg.getBytes(StandardCharsets.UTF_8)); //write UTF-8
                    outputStream.writeChar('\n');
                }
            }
        } else {
            System.out.println("========== CREATING");
            try(FSDataOutputStream outputStream = fs.create(path)){
                for (String msg : buffer) {
                    outputStream.write(msg.getBytes(StandardCharsets.UTF_8)); //write UTF-8
                    outputStream.writeChar('\n');
                }
            }
        }
        return true;
    }

    public HDFSWriter() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop-namenode:9820");
        fs = FileSystem.get(conf);
    }
}
