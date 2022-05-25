package com.example.scheduler;

import com.example.QueryLayer.QueryHandler;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.Socket;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Timer;
import java.util.TimerTask;

public class Scheduler {

    private static Spark spark_main = null;
    private static Spark spark_temp = null;
    private static String mapred_path = null;
    private static final Object lock = new Object();


    public static void main() {
        // set timer. As soon as timer finishes, spark_temp should be useless
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {

                int new_port = spark_main.getPort() == 10101? 10102:10101;
                LocalDateTime time = LocalDateTime.now();
                DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy_MM_dd_hh");

                try {
                    String spark_path = "/user/real_view_"+fmt.format(time);
                    Spark temp = new Spark(spark_path, new_port);
                    synchronized (lock) {
                        spark_temp = spark_main;
                        spark_main = temp;
                    }

                    // TODO run MapReduce
                    String batch_path = "../batch_view_"+fmt.format(time);
                    Runtime.getRuntime().exec("").waitFor();

                    // TODO tell backend
                    QueryHandler update_paths = new QueryHandler();
                    update_paths.loadView(batch_path,spark_path);
                    // delete old real time view
                    synchronized (lock) {
                        spark_temp.delete();
                        spark_temp = null;
                    }

                    // delete old batch view
                    if(mapred_path != null)
                        FileUtils.deleteDirectory(new File(mapred_path));
                    mapred_path = batch_path;

                } catch (IOException | InterruptedException | SQLException e) {
                    e.printStackTrace();
                }
            }
        }, 0, 60*60*1000);


        try {
            // TODO port number
            Socket socket = new Socket("localhost", 3000);
            BufferedReader br;
            br = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            while (true){
                String data = br.readLine();
                synchronized (lock) {
                    spark_main.send(data);
                    if (spark_temp != null)
                        spark_temp.send(data);
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
