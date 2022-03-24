package org.example;

import java.io.IOException;
import java.net.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.concurrent.*;

public class Receiver {

    private int port;
    private String ip;
    private String[] msgBuffer;
    private int counter;
    private DatagramSocket socket;
    private String recentDate;
    static HDFSWriter2 writer = null;
    static ExecutorService executor = Executors.newSingleThreadExecutor();
    //public static BlockingQueue<Long> arrivalTime = new LinkedBlockingQueue<>(); //added this for stats..
    // but need some more thinking

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

        Receiver r = new Receiver("localhost", 3500);
        r.receive();
    }

    public Receiver(String ip, int port) throws IOException {
        System.out.println("Starting Receiver...");
        this.ip = ip;
        this.port = port;
        this.msgBuffer = new String[1024];
        this.counter = 0;
        this.recentDate = this.getDate();
        System.out.println("in constructor " + this.recentDate);
        try {
            this.socket = new DatagramSocket(port, InetAddress.getByName(ip));
        } catch (SocketException | UnknownHostException e) {
            System.out.println("Failed to create socket!");
            e.printStackTrace();
        }

        writer = new HDFSWriter2();
    }


    /*
    * an infinite while loop to keep receiving health messages
    * */
    public void receive() throws IOException {
        System.out.println("Waiting for messages");
        byte[] bfr = new byte[1024];
        while(true){
            DatagramPacket rcvPkt = new DatagramPacket(bfr, bfr.length);
            socket.receive(rcvPkt);
            //arrivalTime.add(System.currentTimeMillis());
            //display received
            String received = new String(rcvPkt.getData(), 0, rcvPkt.getLength());
            //System.out.println("received string " + this.counter + ": "+ received);
            this.bufferMsg(received);
        }
    }

    /*
    * buffer messages until there are 1024, then send to HDFS writer
    * */
    private void bufferMsg(String msg) {
        this.msgBuffer[this.counter] = msg;
        if(this.counter >= 1023 || this.getDate().compareTo(this.recentDate) != 0){
            System.out.println("current : " + this.getDate() + ", recent : " + this.recentDate + ", received : " + this.counter);
            this.recentDate = this.getDate();
            System.out.println("********************************************************************************************************************************");

            String[] temp = Arrays.copyOfRange(this.msgBuffer, 0,this.counter);
            executor.execute(()-> {
                System.out.println("buffer >>> " + temp.length);
                try {
                    long start = System.currentTimeMillis(), end;
                    writer.write(temp, "0006.log");
                    end = System.currentTimeMillis();
                    System.out.printf("Writing in HDFS: %f records/second\n", (double)temp.length/(end-start));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            /*String[] temp = Arrays.copyOfRange(msgBuffer, 0,counter);
            try {
                writer.write(temp, "2019.log");
            } catch (IOException e) {
                e.printStackTrace();
            }*/
            msgBuffer = new String[1024];
            this.counter = -1;
        }
        this.counter = (this.counter + 1) % 1024;
    }

    private String getDate(){
        LocalDateTime myDateObj = LocalDateTime.now();
        DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("dd_MM_yyyy");
        return myDateObj.format(myFormatObj) + ".log";
    }
}
