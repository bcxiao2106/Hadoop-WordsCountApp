package wordscountapp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.util.Iterator;
import java.io.IOException;


public class DominantCalcReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter r) throws IOException {
		//input: [<0:state>, <1:keyword, 2:count>]
		int valueInt = 0;
		while(values.hasNext()){
			valueInt += values.next().get();	
		}
		//output: [<0:keyword>, <1:dominant count>]
		output.collect(key, new IntWritable(valueInt));
	}
}

