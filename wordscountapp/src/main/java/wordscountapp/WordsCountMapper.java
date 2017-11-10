package wordscountapp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.io.IOException;

/*
 * Author: BICHENG XIAO
 * UCID: bx34
 * Email: bx34@njit.edu
 * */

public class WordsCountMapper extends MapReduceBase implements Mapper<Object, Text, KeyWordsWritable, IntWritable>{
    //private Text state = new Text(); 
    private final static IntWritable one = new IntWritable(1);
    
	public void map(Object key, Text value, OutputCollector<KeyWordsWritable,IntWritable> output, Reporter r) throws IOException {
		String fileName = ((FileSplit) r.getInputSplit()).getPath().getName().split("\\.")[0];
		//state = new Text();//get state from the filename
		String text = value.toString();
		text = text.replaceAll( "[^A-Za-z ]", " " ).toLowerCase();//filter the text
		
		for (String keyword: text.split(" ")) {
			if (keyword.length() >= 6) {
				if(keyword.equalsIgnoreCase("education") || keyword.equalsIgnoreCase("politics") || 
						keyword.equalsIgnoreCase("sports") || keyword.equalsIgnoreCase("agriculture")) {
					//output: [<0:keyword, 1:state>, <2:count>]
					output.collect(new KeyWordsWritable(new Text(fileName),new Text(keyword)), one);
				}
			}
		}
	}
}
