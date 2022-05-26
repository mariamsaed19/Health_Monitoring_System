package com.example.scheduler;

import com.example.QueryLayer.QueryHandler;
import com.example.servingLayer.BatchViewGenerator;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Timer;
import java.util.TimerTask;

public class Scheduler {

    private static Spark spark_main = null;
    private static Spark spark_temp = null;
    private static final Object lock = new Object();


    public static void main() {
        // set timer. As soon as timer finishes, spark_temp should be useless
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {

                int new_port = spark_main == null || spark_main.getPort() == 10101? 10102:10101;
                LocalDateTime time = LocalDateTime.now();
                DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy_MM_dd_hh");
                try {
                    String spark_path = "local_data/real";
                    Spark temp = new Spark(spark_path, new_port);
                    synchronized (lock) {
                        spark_temp = spark_main;
                        spark_main = temp;
                    }

                    // call MapReduce's main
                    String master_path = "/user/master_dataset/";
                    String batch_path = "/user/batch_view";
                    BatchViewGenerator.main(new String[]{master_path, batch_path});
                    System.out.println("done with maaaap reduuuce");


                    // reload new batch views
                    QueryHandler update_paths = new QueryHandler();
                    update_paths.loadView(batch_path);

                    // delete old real time view
                    synchronized (lock) {
                        spark_temp.delete();
                        spark_temp = null;
                    }

                } catch (IOException | InterruptedException | SQLException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 0, 15*60*1000);


        try {
            ServerSocket serverSocket = new ServerSocket(7777);
            System.out.println("Waiting...");
            Socket socket = serverSocket.accept();
            System.out.println("Accepted connection : " + socket);
            BufferedReader br;
            br = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            while (true){
                String data = br.readLine();
                System.out.println("received data : " + data);
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
