package com.example.QueryLayer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.RemoteIterator;

public class QueryHandler {

    private Connection conn;
    private File localPath;
    private FileSystem loader;

    public QueryHandler() throws SQLException, IOException {
        //create a connection
        this.conn = DriverManager.getConnection("jdbc:duckdb:");

        //configure file system
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop-namenode:9820");
        this.loader = FileSystem.get(conf);

        //setting local paths
        this.localPath = new File("local_data/batch/");
    }

    //batchPath and realtimePath are directories in hdfs
    public void loadView(String batchPath) throws IOException {
        // copy to local
        Path batch = new Path(batchPath);

        System.out.println(" >>>>>> new data loaded to" + this.localPath);
        FileUtils.cleanDirectory(this.localPath);
        this.copyToLocal(batch);
    }

    private void copyToLocal(Path hdfsPath) throws IOException {
        Path local = new Path(this.localPath.getAbsolutePath());

        RemoteIterator<LocatedFileStatus> fileStatusListIterator = this.loader.listFiles(hdfsPath, true);
        while(fileStatusListIterator.hasNext()){
            LocatedFileStatus fileStatus = fileStatusListIterator.next();
            this.loader.copyToLocalFile(false, fileStatus.getPath(), local, true);
        }
    }


    public List<String> query(String start, String end){
        //query for averages and count
        PreparedStatement p_stmt = null;
        try {
            p_stmt = conn.prepareStatement("SELECT  service_name,count(*), arg_max(cpu_peak_time,cpu_peak_util),arg_max(disk_peak_time,disk_peak_util),arg_max(ram_peak_time,ram_peak_util),sum(count), avg(avg_cpu), avg(avg_disk ), avg(avg_ram) FROM '" + this.localPath.getAbsolutePath() + "\\*\\*.parquet' WHERE timestamp BETWEEN  ? AND ? GROUP BY service_name;");
            p_stmt.setString(1, start);
            p_stmt.setString(2, end);
            ResultSet rs = p_stmt.executeQuery();
            ResultSetMetaData metadata = rs.getMetaData();
            int columnCount = metadata.getColumnCount();

            List<String> query_res = new ArrayList<>();
            while (rs.next()) {
                String row = "";
                for (int i = 1; i <= columnCount; i++) {
                    row += rs.getString(i) + ",";
                }
                query_res.add(row);
            }
            System.out.println(" >>>>>> done with query processing");
            return query_res;
        } catch (SQLException throwables) {
            return new ArrayList<String>();
        }
    }
}
