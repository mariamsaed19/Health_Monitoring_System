package com.example.QueryLayer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.RemoteIterator;

public class QueryHandler {

    private Connection conn;
    private File localPath;
    private FileSystem loader;


    public static void main(String args[]) throws IOException, SQLException {
        QueryHandler t = new QueryHandler();
        String real = "../batch_view/";
        String batch = "../test_output/";

        String start = "2022_03_22_10_45";
        String end = "2022_04_09_18_59";

        t.loadView(batch,real);
        long s = System.currentTimeMillis();
        String[] res = t.query(start, end);
        for(int i=0;i<res.length;i++){
            System.out.println(res[i]);
        }
        long f = System.currentTimeMillis();
        long timeElapsed = f - s;
        System.out.println("Time >> "+timeElapsed + " ms");
    }


    public QueryHandler() throws SQLException, IOException {
        //create a connection
        this.conn = DriverManager.getConnection("jdbc:duckdb:");

        //configure file system
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop-namenode:9820");
        this.loader = FileSystem.get(conf);

        //setting local paths
        this.localPath = new File("local_data/");

        System.out.println(this.localPath.getAbsolutePath());
    }

    //batchPath and realtimePath are directories in hdfs
    public void loadView(String batchPath ,String realtimePath) throws IOException {
        // copy to local
        Path batch = new Path(batchPath);
        Path realtime = new Path(realtimePath);

        FileUtils.cleanDirectory(this.localPath);
        this.copyToLocal(batch);
        this.copyToLocal(realtime);
    }

    private void copyToLocal(Path hdfsPath) throws IOException {
        Path local = new Path(this.localPath.getAbsolutePath());

        RemoteIterator<LocatedFileStatus> fileStatusListIterator = this.loader.listFiles(hdfsPath, true);
        while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            this.loader.copyToLocalFile(false, fileStatus.getPath(), local, true);
        }
    }


    //TODO: assuming start and end have the right format
    public String[] query(String start, String end) throws SQLException {
        //query for averages and count
        PreparedStatement p_stmt = conn.prepareStatement("SELECT  service_name,count(*), arg_max(cpu_peak_time,cpu_peak_util),arg_max(disk_peak_time,disk_peak_util),arg_max(ram_peak_time,ram_peak_util),sum(count), avg(avg_cpu), avg(avg_disk ), avg(avg_ram) FROM '" + this.localPath.getAbsolutePath() + "\\*.parquet' WHERE timestamp BETWEEN  ? AND ? GROUP BY service_name;");

        p_stmt.setString(1, start);
        p_stmt.setString(2, end);
        ResultSet rs = p_stmt.executeQuery();
        ResultSetMetaData metadata = rs.getMetaData();
        int columnCount = metadata.getColumnCount();

        ArrayList<String> query_res = new ArrayList<>();
        while (rs.next()) {
            String row = "";
            for (int i = 1; i <= columnCount; i++) {
                row += rs.getString(i) + ",";
            }
            query_res.add(row);
            //System.out.println(row);
        }
        System.out.println("***************************");
        String[] res = new String[query_res.size()];
        return query_res.toArray(res);

    }
}
