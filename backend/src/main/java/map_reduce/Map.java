package map_reduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import java.io.IOException;
import java.util.StringTokenizer;

public class Map extends Mapper<LongWritable, Text,Text, Text> {
    public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException{
        String line = value.toString();
        JSONObject json = new JSONObject(line);
        String serviceName = json.getString("serviceName");
        //System.out.println("Map >> " + serviceName + "  :  " + json.toString());
        context.write(new Text(serviceName), new Text(json.toString()));
        //System.out.println("End Map");
    }
}
