package wordscountapp;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DominantCalcMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable>{ 
	public void map(Object key, Text value, OutputCollector<Text,IntWritable> output, Reporter r) throws IOException {
		//input: [<0:keyword>, <1:state>]
		String keywordStateStrArr [] = key.toString().split(",");
		String keyword = keywordStateStrArr[0];
		//output: [<0:state>, <1:keyword, 2:count>]
		System.out.println("**********" + keyword + " : " + 1);
		output.collect(new Text(keyword), new IntWritable(1));
	}
}
