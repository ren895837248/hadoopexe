package hadoopexe;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yangshan on 2017/6/5.
 */
public class MaxTemperautreMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String year = line.substring(15,19);

        String airTemperauter = line.substring(87,92);

        if(!missing(airTemperauter)){
            context.write(new Text(year),new IntWritable(Integer.parseInt(airTemperauter)));
        }

    }

    private boolean missing(String airTemperauter) {
        return "+9999".equals(airTemperauter);
    }

}
