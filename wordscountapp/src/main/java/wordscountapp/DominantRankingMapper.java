package wordscountapp;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DominantRankingMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text>{ 
	public void map(Object key, Text value, OutputCollector<Text,Text> output, Reporter r) throws IOException {
		//input: [<0:rankingList>, <1:state>]
		String keywordCountStrArr [] = key.toString().split(",");
		//output: [<0:state, 1:count>, <2:keyword>]
		output.collect(new Text(key.toString()), new Text(value));
	}
}
