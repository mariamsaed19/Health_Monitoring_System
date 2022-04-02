package map_reduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;

import java.io.IOException;

public class Reduce extends Reducer<Text, Text, NullWritable,Text>{
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
        float avgCpu = 0;
        float avgDisk = 0;
        float avgRAM = 0;

        float maxUtilCpu = 0;
        float maxUtilCpuTime = 0;

        float maxUtilDisk = 0;
        float maxUtilDiskTime = 0;

        float maxUtilRam = 0;
        float maxUtilRamTime = 0;

        int counter = 0;
        for(Text jsonText : values){
            //parse json
            JSONObject json = new JSONObject(jsonText.toString());

            long timestamp = json.getLong("Timestamp");     //extract timestamp

            float cpu = json.getFloat("CPU");        //extract cpu percentage

            JSONObject disk = json.getJSONObject("Disk");
            float totalDisk = disk.getFloat("Total");
            float freeDisk = disk.getFloat("Free");
            float utilDisk = (totalDisk - freeDisk)/totalDisk;      //find disk utilization : used/total

            JSONObject ram = json.getJSONObject("RAM");
            float totalRam = ram.getFloat("Total");
            float freeRam = ram.getFloat("Free");
            float utilRam = (totalRam - freeRam)/totalRam;      //find RAM utilization : used/total

            avgCpu += cpu;
            avgDisk += utilDisk;
            avgRAM += utilRam;

            // update max cpu utilization timestamp
            if(cpu >= maxUtilCpu){
                maxUtilCpu = cpu;
                maxUtilCpuTime = timestamp;
            }

            // update max disk utilization timestamp
            if(utilDisk >= maxUtilDisk){
                maxUtilDisk = utilDisk;
                maxUtilDiskTime = timestamp;
            }

            // update max RAM utilization timestamp
            if(utilRam >= maxUtilRam){
                maxUtilRam = utilRam;
                maxUtilRamTime = timestamp;
            }

            counter++;
        }

        avgCpu /= counter;
        avgDisk /= counter;
        avgRAM /= counter;

        //statistics json
        JSONObject stats = new JSONObject();
        stats.put("serviceName", key.toString());
        stats.put("cpu", avgCpu);
        stats.put("disk", avgDisk);
        stats.put("ram", avgRAM);
        stats.put("cpu_peak_time", maxUtilCpuTime);
        stats.put("cpu_peak_util", maxUtilCpu);
        stats.put("disk_peak_time", maxUtilDiskTime);
        stats.put("disk_peak_util", maxUtilDisk);
        stats.put("ram_peak_time", maxUtilRamTime);
        stats.put("ram_peak_util", maxUtilRam);
        stats.put("count", counter);

        // I want the final file to have multiple lines,
        // each line containing a JSON object of the form {serviceName: .., cpu: .. etc}
        context.write(NullWritable.get(), new Text(stats.toString()));
    }
}
