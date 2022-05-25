package MapReduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.Calendar;

public class Map extends Mapper<LongWritable, Text, Text, Text>{

    public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException{
        System.out.println("** Map ** : ");
        // parse csv line
        String line = value.toString();
        String row[] = line.split(",");

        // extract key
        String serviceName = row[0];
        long timestamp = Long.parseLong(row[1]);
        String dateKey = getKey(timestamp);
        //System.out.println("Map >> " + serviceName + "  :  " + json.toString());
        //System.out.println(" >>> inside mapper >>> " + timestamp);
        context.write(new Text(serviceName + "," + dateKey), new Text(line));
        //System.out.println("End Map");
    }

    // format the timestamp as YYYY_MM_DD_HH_mm
    private String getKey(long timestamp){
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(timestamp*1000);

        String y = Integer.toString(calendar.get(Calendar.YEAR));
        int mon = calendar.get(Calendar.MONTH) + 1;
        int d = calendar.get(Calendar.DAY_OF_MONTH);
        int h = calendar.get(Calendar.HOUR_OF_DAY);
        int m = calendar.get(Calendar.MINUTE);

        return y + "_" + String.format("%02d", mon) + "_" + String.format("%02d", d) + "_" + String.format("%02d", h) + "_" + String.format("%02d", m);
    }
}
