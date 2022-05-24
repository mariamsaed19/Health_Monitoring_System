package com.example.scheduler;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;


public class Spark {
    private String path;
    private int port;
    private ServerSocket socket;
    private Process instance;
    private Socket client;
    private PrintWriter pw;

    Spark(String path, int port) throws IOException {
        this.path = path;
        this.port = port;
        this.socket = new ServerSocket(this.port);
        this.instance = Runtime.getRuntime().exec(""); //TODO
        client = socket.accept();
        pw = new PrintWriter(client.getOutputStream(),true);
    }

    public void send(String data){
        pw.println(data);
    }

    public int getPort(){
        return this.port;
    }

    public void delete() throws InterruptedException, IOException {
        // end instance
        instance.destroyForcibly();
        // close port
        pw.close();
        client.close();
    }
}
