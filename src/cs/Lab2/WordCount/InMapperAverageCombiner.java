package cs.Lab2.WordCount;


import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

public class InMapperAverageCombiner extends Mapper<LongWritable, Text, Text, TemperatureAveragingPair> {
	 //sample line of weather data
	 //0029029070999991901010106004+64333+023450FM-12+000599999V0202701N015919999999N0000001N9-00781+99999102001ADDGF10899199999999999


	    private static final int MISSING = 9999;
	    private Map<String,TemperatureAveragingPair> pairMap = new HashMap<String,TemperatureAveragingPair>();


	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        String yearMonth = line.substring(15, 21);

	        int tempStartPosition = 87;

	        if (line.charAt(tempStartPosition) == '+') {
	            tempStartPosition += 1;
	        }

	        int temp = Integer.parseInt(line.substring(tempStartPosition, 92));

	        if (temp != MISSING) {
	            TemperatureAveragingPair pair = pairMap.get(yearMonth);
	            if(pair == null){
	                pair = new TemperatureAveragingPair();
	                pairMap.put(yearMonth,pair);
	            }
	            int temps = pair.getTemp().get() + temp;
	            int count = pair.getCount().get() + 1;
	            pair.set(temps,count);
	        }
	    }


	    @Override
	    protected void cleanup(Context context) throws IOException, InterruptedException {
	        Set<String> keys = pairMap.keySet();
	        Text keyText = new Text();
	        for (String key : keys) {
	             keyText.set(key);
	             context.write(keyText,pairMap.get(key));
	        }
	    }
	}

