package MapReduce;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.sql.Timestamp;

public class Reduce extends Reducer<Text, Text, Void, GenericRecord> {
    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException,InterruptedException {
        System.out.println(" *** reduce *** " + key.toString());
        float avgCpu = 0;
        float avgDisk = 0;
        float avgRAM = 0;

        float maxUtilCpu = 0;
        long maxUtilCpuTime = 0;

        float maxUtilDisk = 0;
        long maxUtilDiskTime = 0;

        float maxUtilRam = 0;
        long maxUtilRamTime = 0;

        long counter = 0;

        for(Text rowText : values){
            //parse csv row
            String row[] = rowText.toString().split(",");

            long timestamp = Long.parseLong(row[1]);     //extract timestamp
            float cpu = Float.parseFloat(row[2]);        //extract cpu percentage
            float totalRam = Float.parseFloat(row[3]);
            float freeRam = Float.parseFloat(row[4]);
            float utilRam = (totalRam - freeRam)/totalRam;      //find RAM utilization : used/total
            float totalDisk = Float.parseFloat(row[5]);
            float freeDisk = Float.parseFloat(row[6]);
            float utilDisk = (totalDisk - freeDisk)/totalDisk;      //find disk utilization : used/total


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


        //write record in the output parquet file
        String keys[] = key.toString().split(",");
        GenericRecord stats = new GenericData.Record(Main.SCHEMA);
        stats.put("service_name", keys[0]);
        stats.put("timestamp", keys[1]);
        stats.put("avg_cpu", avgCpu);
        stats.put("avg_disk", avgDisk);
        stats.put("avg_ram", avgRAM);
        stats.put("cpu_peak_time", new Timestamp(maxUtilCpuTime*1000).toString());
        stats.put("cpu_peak_util", maxUtilCpu);
        stats.put("disk_peak_time", new Timestamp(maxUtilDiskTime*1000).toString());
        stats.put("disk_peak_util", maxUtilDisk);
        stats.put("ram_peak_time", new Timestamp(maxUtilRamTime*1000).toString());
        stats.put("ram_peak_util", maxUtilRam);
        stats.put("count", counter);

        context.write(null, stats);
    }

}