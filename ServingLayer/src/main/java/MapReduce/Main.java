package MapReduce;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;


public class Main extends Configured implements Tool{
    public static void main(String[] args)  throws Exception{
        int exitFlag = ToolRunner.run(new Main(), args);
        System.exit(exitFlag);
    }
    /// Schema
    protected static final Schema SCHEMA = new Schema.Parser().parse(
            "{\n" +
                    "	\"type\":	\"record\",\n" +
                    "	\"namespace\":	\"master_data\",\n" +
                    "	\"name\":	\"testFile\",\n" +
                    "	\"fields\":\n" +
                    "	[\n" +
                    "			{\"name\": \"service_name\",	\"type\":	\"string\"},\n"+
                    "			{\"name\":	\"timestamp\", \"type\":	\"string\"},\n"+
                    "			{\"name\":	\"avg_cpu\", \"type\":	\"float\"},\n"+
                    "			{\"name\":	\"avg_disk\", \"type\":	\"float\"},\n"+
                    "			{\"name\":	\"avg_ram\", \"type\":	\"float\"},\n"+
                    "			{\"name\":	\"cpu_peak_time\", \"type\":	\"string\"},\n"+
                    "			{\"name\":	\"cpu_peak_util\", \"type\":	\"float\"},\n"+
                    "			{\"name\":	\"disk_peak_time\", \"type\":	\"string\"},\n"+
                    "			{\"name\":	\"disk_peak_util\", \"type\":	\"float\"},\n"+
                    "			{\"name\":	\"ram_peak_time\", \"type\":	\"string\"},\n"+
                    "			{\"name\":	\"ram_peak_util\", \"type\":	\"float\"},\n"+
                    "			{\"name\":	\"count\", \"type\":	\"long\"}\n"+
                    "	]\n"+
                    "}\n");



    public int run(String[] args) throws Exception {
        System.out.println("Start ....");
        String[] paths = {"/user/master_dataset/", "/user/batch_view/"};  //set input and output paths for MapReduce
        //set configurations
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadoop-namenode:9820");
        Job job = Job.getInstance(conf, "parquet");

        job.setJarByClass(Main.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);

        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        // setting schema to be used
        AvroParquetOutputFormat.setSchema(job, SCHEMA);
        FileInputFormat.addInputPath(job, new Path(paths[0]));
        FileOutputFormat.setOutputPath(job, new Path(paths[1]));

//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println("MapReduce is launched.");
        Path outputPath = new Path(paths[1]);
//        Path outputPath = new Path(args[1]);

        outputPath.getFileSystem(conf).delete(outputPath);
        return job.waitForCompletion(true) ? 0 : 1;
    }

}