package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

public class Loader {
    public static void main(String args[]) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop-namenode:9820");
        conf.set("dfs.replication", "1"); //might remove the bad datanode error
        FileSystem fs = FileSystem.get(conf);
        String filePath = new File("").getAbsolutePath();
        Path p = new Path(filePath);

        long startTime = System.currentTimeMillis();

        fs.copyFromLocalFile(new Path(p.getParent() + "/input/"),
                new Path("/user/"));

        long end_to_end_time = System.currentTimeMillis() - startTime;
        System.out.println("latency : " + end_to_end_time);
    }
}
