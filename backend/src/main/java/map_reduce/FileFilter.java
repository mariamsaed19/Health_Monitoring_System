package map_reduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


public class FileFilter extends Configured implements PathFilter{
    Configuration conf;
    FileSystem fs;

    @Override
    public boolean accept(Path path) {
        try {
            if (fs.isDirectory(path)) {
                return true;
            } else {
                DateFormat DFormat = new SimpleDateFormat("dd_MM_yyyy");
                Date start = DFormat.parse(conf.get("start_date"));
                Date end = DFormat.parse(conf.get("end_date"));
                String currentStr = path.getName().substring(0, path.getName().lastIndexOf("."));
                Date currentFile = DFormat.parse(currentStr);
                //System.out.println(path.getName() + " : " + (!currentFile.after(end) && !currentFile.before(start)));
                return !currentFile.after(end) && !currentFile.before(start);
            }
        } catch (IOException | ParseException e) {
            return false;
        }

    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        if (conf != null) {
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
