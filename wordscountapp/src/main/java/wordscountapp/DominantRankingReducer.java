package wordscountapp;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import java.util.*;
import java.io.IOException;


public class DominantRankingReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter r) throws IOException {
	
		//input: [<0:state>, <1:keyword, 2:count>]
		//Store values into map
		HashMap<Integer, String> map = new HashMap<Integer, String>();
		String keyword = "";
		int i = 0;
		int[] rankingIdx = new int[4];
		while(values.hasNext() & i < 4) {
			String valueStr = values.next().toString();
			String[] valueArr = valueStr.split(",");
			int count = Integer.parseInt(valueArr[1]);
			rankingIdx[i] = count;
			keyword = valueArr[0];
			map.put(count, keyword);
			i++;
		}
		Arrays.sort(rankingIdx);
		String rankingList = "";
		for(int j=0; j < rankingIdx.length; j++){
			String keyW = map.get(rankingIdx[j]);
			if(keyW != null)
			rankingList += keyW + " ";
		}
		
		//output: [<0:rankingList>, <1:state>]
		output.collect(new Text(rankingList), new Text(key.toString()));
	}
}

