package org.example;
import java.io.IOException;
import java.net.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Receiver {

    private int port;
    private String ip;
    private String[] msgBuffer;
    private int counter;
    private DatagramSocket socket;
    private String recentDate;
    HDFSWriter writer = null;

    public static void main(String[] args) throws IOException {
        Receiver r = new Receiver("10.0.6.165", 3500);
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

        writer = new HDFSWriter();
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
            System.out.println("received string " + this.counter + ": "+ received);
            this.bufferMsg(received);
        }
    }

    /*
    * buffer messages until there are 1024, then send to HDFS writer
    * */
    private void bufferMsg(String msg) throws IOException {
        this.msgBuffer[this.counter] = msg;
        if(this.counter >= 1023 || this.getDate().compareTo(this.recentDate) != 0){
            System.out.println("current : " + this.getDate() + ", recent : " + this.recentDate);
            this.recentDate = this.getDate();
            this.counter = -1;
            System.out.println("********************************************************************************************************************************");
            writer.write(msgBuffer, recentDate);
            // TODO zero out the msgbuffer
        }
        this.counter = (this.counter + 1) % 1024;
    }

    private String getDate(){
        LocalDateTime myDateObj = LocalDateTime.now();
        DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("dd_MM_yyyy");
        return myDateObj.format(myFormatObj) + ".log";
    }
}
