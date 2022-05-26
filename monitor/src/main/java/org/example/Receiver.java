package org.example;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
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
    private Socket tcpSocket;
    private String recentDate;
    private final HDFSWriter writer;
    private final ExecutorService executor;
    private final int schedulerPort = 7777;



    public Receiver(String ip, int port, HDFSWriter writer, ExecutorService executor) throws IOException {
        System.out.println("Starting Receiver...");
        this.ip = ip;
        this.port = port;
        this.msgBuffer = new String[1024];
        this.counter = 0;
        this.recentDate = this.getDate();
        this.writer = writer;
        this.executor = executor;
        try {
            this.socket = new DatagramSocket(port, InetAddress.getByName(ip));
            this.tcpSocket = new Socket(ip, this.schedulerPort);
        } catch (SocketException | UnknownHostException e) {
            System.out.println("Failed to create socket!");
            e.printStackTrace();
        }
    }


    /*
    * an infinite while loop to keep receiving health messages
    * */
    public void receive() throws IOException {
        System.out.println("Waiting for messages");
        byte[] bfr = new byte[1024];
        while(true){
            DatagramPacket rcvPkt = new DatagramPacket(bfr, bfr.length);
            this.socket.receive(rcvPkt);
            //display received
            String received = new String(rcvPkt.getData(), 0, rcvPkt.getLength());
            System.out.println(received);
            this.bufferMsg(received);
            //send data to scheduler
            OutputStream output = this.tcpSocket.getOutputStream();
            PrintWriter writer = new PrintWriter(output, true);
            writer.println(received);
        }
    }

    /*
    * buffer messages until there are 1024, then send to HDFS writer
    * */
    private void bufferMsg(String msg) {
        this.msgBuffer[this.counter] = msg;

        if (this.counter == 0){ // first message in batch
            Main.arrivalTime.add(System.currentTimeMillis());
        }

        if(this.counter >= 1023 || this.getDate().compareTo(this.recentDate) != 0){
            this.recentDate = this.getDate();
            System.out.println("****************************" + "file : " + this.getDate() + ", received : " + this.counter + "*******************************************");
            String[] temp = Arrays.copyOfRange(this.msgBuffer, 0,this.counter + 1);
            String temp_date = this.recentDate;
            executor.execute(()-> {
                try {
                    long start = System.currentTimeMillis(), end;
                    writer.write(temp, temp_date);
                    end = System.currentTimeMillis();
                    System.out.printf("Writing in HDFS: %f records/second\n", (double)temp.length/(end-start));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            msgBuffer = new String[1024];
            this.counter = -1;
        }
        this.counter = (this.counter + 1) % 1024;
    }

    private String getDate(){
        LocalDateTime myDateObj = LocalDateTime.now();
        DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("dd_MM_yyyy");
        return myDateObj.format(myFormatObj) + ".csv";
    }
}
