package HealthMsgManager;
import org.json.JSONObject;
import java.io.IOException;
import java.net.*;

public class Receiver {

    private int port;
    private String ip;
    private String[] msgBuffer;
    private int counter;
    private DatagramSocket socket;

    public static void main(String[] args) throws IOException {
        Receiver r = new Receiver("192.168.1.9", 3500);
        r.receive();
    }

    public Receiver(String ip, int port){
        System.out.println("Starting Receiver...");
        this.ip = ip;
        this.port = port;
        this.msgBuffer = new String[1024];
        this.counter = 0;
        try {
            this.socket = new DatagramSocket(port, InetAddress.getByName(ip));
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
    private void bufferMsg(String msg){
        this.msgBuffer[this.counter] = msg;
        if(this.counter >= 1023){
            //TODO: send to HDFS writer
            System.out.println("********************************************************************************************************************************");
        }
        this.counter = (this.counter + 1) % 1024;
    }
}
