package org.example;

import java.io.IOException;
import java.util.concurrent.*;

public class Main {
    static HDFSWriter writer = null;
    static ExecutorService executor = Executors.newSingleThreadExecutor();
    public static BlockingQueue<Long> arrivalTime = new LinkedBlockingQueue<>(); //added this for stats..
    public static double mean = 0, sum_sq = 0, std = 0;
    public static long batches = 0;

    public static void main(String[] args) throws IOException {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down, please wait while writing jobs are finished. " +
                    "Allow up to 5 minutes.");
            executor.shutdown();
            try {
                while(!executor.awaitTermination(5, TimeUnit.MINUTES)){
                    System.out.println("5 minutes have passed but the tasks are still running. Please wait.");
                }
                if (writer != null)
                    writer.close();
            } catch (InterruptedException e) {
                System.out.println("Do you want to lose all your hard work?!");
                e.printStackTrace();
            } catch (IOException e) {
                System.out.println("Writer doesn't want to close. Give it something more to write :(");
                e.printStackTrace();
            }

        }));

        writer = new HDFSWriter();
        Receiver r = new Receiver("192.168.1.7", 3500, writer, executor);
        r.receive();
    }
}
