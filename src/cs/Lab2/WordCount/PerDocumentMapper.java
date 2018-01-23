package cs.Lab2.WordCount;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;


public class PerDocumentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        IntWritable writableCount = new IntWritable();
        Text text = new Text();
        Map<String,Integer> tokenMap = new HashMap<String, Integer>();
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        while(tokenizer.hasMoreElements()){
            String token = tokenizer.nextToken();
            Integer count = tokenMap.get(token);
            if(count == null) count = new Integer(0);
            count+=1;
            tokenMap.put(token,count);
        }

        Set<String> keys = tokenMap.keySet();
        for (String s : keys) {
             text.set(s);
             writableCount.set(tokenMap.get(s));
             context.write(text,writableCount);
        }
    }
}