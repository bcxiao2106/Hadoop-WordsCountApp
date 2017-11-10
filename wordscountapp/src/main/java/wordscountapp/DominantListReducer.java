package wordscountapp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.util.*;
import java.util.Iterator;
import java.io.IOException;


public class DominantListReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter r) throws IOException {
		//input: [<0:state>, <1:keyword, 2:count>]
		//Store values into map
		HashMap<String, String> map = new HashMap<String, String>();
		String dominantKeywords = "";
		int maxCount = 0;
		while(values.hasNext()) {
			String valueStr = values.next().toString();
			String[] valueArr = valueStr.split(",");
			int count = Integer.parseInt(valueArr[1]);
			if(count > maxCount){
				maxCount = count;
				dominantKeywords = valueArr[0];
			}	
		}
		map.put(dominantKeywords, key.toString());
		//output: [<0:keyword>, <1:state>]
		System.out.println(" ===Dominant===== : " + dominantKeywords + " : " + key.toString());
		output.collect(new Text(dominantKeywords), new Text(key.toString()));
	}
}
