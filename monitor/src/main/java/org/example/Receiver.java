package org.example;
import java.io.IOException;
import java.net.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Receiver {

    private int port;
    private String ip;
    private String[] msgBuffer;
    private int counter;
    private DatagramSocket socket;
    private String recentDate;
    HDFSWriter2 writer = null;
    ExecutorService executor = Executors.newSingleThreadExecutor();

    public static void main(String[] args) throws IOException {
        Receiver r = new Receiver("10.0.1.57", 3500);
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


                System.out.println("buffer >>> " + temp.length );
                try {
                    writer.write(temp, "0006.log");
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
