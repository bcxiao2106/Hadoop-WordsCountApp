package wordscountapp;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class DominantListMapper extends MapReduceBase implements Mapper<Object, Text, Text, Text>{ 
	public void map(Object key, Text value, OutputCollector<Text,Text> output, Reporter r) throws IOException {
		//input: [<0:keyword, 1:state>, <2:count>]
		String keywordCountStrArr [] = key.toString().split(",");
		//output: [<0:state>, <1:keyword, 2:count>]
		System.out.println("**********" + keywordCountStrArr[1] + " : " + keywordCountStrArr[0] + "," + keywordCountStrArr[2]);
		output.collect(new Text(keywordCountStrArr[1]), new Text(keywordCountStrArr[0] + "," + keywordCountStrArr[2]));
	}
}
