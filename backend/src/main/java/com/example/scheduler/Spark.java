package com.example.scheduler;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;


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
        String home = System.getenv("SPARK_HOME");
        Path home_path = Paths.get(home);
        Path submit = home_path.resolve("/bin/spark-submit");
        this.instance = Runtime
                .getRuntime()
                .exec(
                        submit +
                                " --class \"org.example.RealTime\" " +
                                "--master local[4] /path/to/RealTime-1.0.jar " +
                                "localhost 10101 /path/to/parquet /path/to/checkpoint"
                ); // TODO modify paths
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
