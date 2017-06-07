package hadoopexe;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by yangshan on 2017/6/5.
 */
public class MaxTemperatureDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf());
        Job job = new Job(jobConf, "Max Temp");
        job.setJarByClass(getClass());
        FileInputFormat.addInputPath(jobConf, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        job.setMapperClass(MaxTemperautreMapper.class);
        job.setCombinerClass(MaxTemperautreReducer.class);
        job.setReducerClass(MaxTemperautreReducer.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        System.exit(
                ToolRunner.run(new MaxTemperatureDriver(),args));
    }


}
