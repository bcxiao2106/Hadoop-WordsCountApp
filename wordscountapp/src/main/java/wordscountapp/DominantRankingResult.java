package wordscountapp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.util.*;
import java.io.IOException;


public class DominantRankingResult extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter r) throws IOException {
	
		//output: [<0:rankingList>, <1:state>]
		String stateList = "";
		while(values.hasNext()) {
			stateList += values.next().toString() + " ";
		}
		
		//output: [<0:rankingList>, <1:stateList>]
		output.collect(key, new Text(stateList));
	}
}