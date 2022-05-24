package map_reduce;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

public class Analyser {

    private final Configuration conf = new Configuration();

    public boolean generateStats(String startDate, String endDate) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Start ....");
        String[] paths = {"/user/input/", "/user/output/"};  //set input and output paths for MapReduce
        //set configurations
        conf.set("fs.defaultFS","hdfs://hadoop-namenode:9820");
        conf.set("dfs.replication", "1");
        conf.set("start_date", startDate);
        conf.set("end_date", endDate);

        //initialize MapReduce Jop
        Job job = new Job(conf,"Service Analyser");

        job.setJarByClass(Analyser.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        Path outputPath = new Path(paths[1]);

        //Configuring the input/output path from the filesystem into the job
        FileInputFormat.setInputPathFilter(job, FileFilter.class);
        FileInputFormat.addInputPath(job, new Path(paths[0]));
        FileOutputFormat.setOutputPath(job, new Path(paths[1]));

        //deleting the output path automatically from hdfs so that we don't have to delete it explicitly
        outputPath.getFileSystem(conf).delete(outputPath);
        //exiting the job only if the flag value becomes false
        System.out.println("MapReduce is launched.");
        return job.waitForCompletion(true) ? true : false;
    }

    public List<String> readResults() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path("/user/output/part-r-00000");
//        List<JSONObject> list = null;
        List<String> list = null;
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))){
//            list = reader.lines().map(JSONObject::new).collect(Collectors.toList());
            list = reader.lines().map(String::new).collect(Collectors.toList());
        }
        return list;
    }
}
