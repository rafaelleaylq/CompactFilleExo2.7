package cs.Lab2.WordCount;

import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;


public class AverageTemperatureReducer extends Reducer<Text, TemperatureAveragingPair, Text, IntWritable> {
    private IntWritable average = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<TemperatureAveragingPair> values, Context context) throws IOException, InterruptedException {
        int temp = 0;
        int count = 0;
        for (TemperatureAveragingPair pair : values) {
            temp += pair.getTemp().get();
            count += pair.getCount().get();
        }
        average.set(temp / count);
        context.write(key, average);
    }
}

