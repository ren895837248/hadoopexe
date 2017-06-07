package hadoopexe;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.util.Arrays;

/**
 * Created by yangshan on 2017/6/5.
 */
public class MaxTemperatureMapperTest {
    @Test
    public void processesVaildRecord(){
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9-00111+99999999999");
        new MapDriver<LongWritable,Text,Text,IntWritable>()
                .withMapper(new MaxTemperautreMapper())
                .withInputValue(value)
                .withOutput(new Text("1950"),new IntWritable(-11))
                .runTest();
    }

    @Test
    public void ingoreMissingTemperatureRecord(){
        Text value = new Text("0043011990999991950051518004+68750+023550FM-12+038299999V0203201N00261220001CN9999999N9+99991+99999999999");
        new MapDriver<LongWritable,Text,Text,IntWritable>()
                .withMapper(new MaxTemperautreMapper())
                .withInputValue(value)
                .runTest();
    }


    public void returnsMaximunInteger(){
        new ReduceDriver<Text,IntWritable,Text,IntWritable>()
                .withReducer(null)
                .withInputKey(new Text("1950"))
                .withInputValues(Arrays.asList( new IntWritable(10),new IntWritable(5)))
                .withOutput(new Text("1950"),new IntWritable(10))
                .runTest();

    }

}
