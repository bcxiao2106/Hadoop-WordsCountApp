package wordscountapp;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * Author: BICHENG XIAO
 * UCID: bx34
 * Email: bx34@njit.edu
 * */

public class WordsCountApp extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new WordsCountApp(), args);
	    System.exit(res);
	}

	public int run(String[] args) throws Exception {
		//Check the user input
		if(args.length != 2){
			System.out.println("Please make sure both INPUT and OUTPUT dir are provided");
			System.out.println("Usage: hadoop jar KC.jar wordscountapp.WordsCountApp INPUT_DIR OUTPUT_DIR");
			System.exit(0);
		}
		
		//Word Counting Map-Reduce: original file => [<"keyword", "state">, <"count">]
		JobConf cnfg = new JobConf(WordsCountApp.class);
		FileSystem fs= FileSystem.get(cnfg); 
		//get the FileStatus list from the directory
		FileStatus[] statesList = fs.listStatus(new Path(args[0]));
		if(statesList != null){
		    for(FileStatus statesFile : statesList){
		        //add each file to the list of inputs for the map-Reduce job
		        FileInputFormat.addInputPath(cnfg, statesFile.getPath());
		    }
		}
		FileOutputFormat.setOutputPath(cnfg, new Path(args[1]+"/KeyWordsCount"));
	      
		cnfg.setMapperClass(WordsCountMapper.class);
		cnfg.setReducerClass(WordsCountReducer.class);
		cnfg.setMapOutputKeyClass(KeyWordsWritable.class);
		cnfg.setMapOutputValueClass(IntWritable.class);
		cnfg.setOutputKeyClass(KeyWordsCatWritable.class);
		cnfg.setOutputValueClass(Text.class);
		cnfg.set("mapred.textoutputformat.ignoreseparator", "true");  
		cnfg.set("mapred.textoutputformat.separator", ",");//use comma as the separator
		
		RunningJob job = JobClient.runJob(cnfg);
		job.waitForCompletion();
		
		/*
		//Ranking Map-Reduce: [<"keyword", "state">, <"count">] => top(sort_by_count([<"keyword">, <"state", "count">]),3)
				if (job.isSuccessful()) {
					JobConf cnfgRanking = new JobConf(WordsCountApp.class);
					fs = FileSystem.get(cnfgRanking); 
					//get FileStatus list from WordCountReducer's output dir
					statesList = fs.listStatus(new Path(args[1]+"/KeyWordsCount"));
					if(statesList != null){
					    for(FileStatus status : statesList){
					        //add each file to the list of inputs for the Map-Reduce job
					    	if (status.getLen() > 0) {
					    		FileInputFormat.addInputPath(cnfgRanking, status.getPath());
							} 
					    }
					}
					FileOutputFormat.setOutputPath(cnfgRanking, new Path(args[1]+"/Ranking"));
				      
					cnfgRanking.setMapperClass(RankingMapper.class);
					cnfgRanking.setReducerClass(RankingReducer.class);
					cnfgRanking.setInputFormat(KeyValueTextInputFormat.class);
					cnfgRanking.setMapOutputKeyClass(Text.class);
					cnfgRanking.setMapOutputValueClass(KeyWordsCatWritable.class);
					cnfgRanking.setOutputKeyClass(Text.class);
					cnfgRanking.setOutputValueClass(Text.class);
					cnfgRanking.set("mapred.textoutputformat.ignoreseparator", "true");  
					cnfgRanking.set("mapred.textoutputformat.separator", ",");//use comma as the separator
					job = JobClient.runJob(cnfgRanking);
					job.waitForCompletion();
				}
		
		//Ranking Map-Reduce: [<"keyword", "state">, <"count">] => top(sort_by_count([<"keyword">, <"state", "count">]),3)
		if (job.isSuccessful()) {
			JobConf cnfgRanking = new JobConf(WordsCountApp.class);
			fs = FileSystem.get(cnfgRanking); 
			//get FileStatus list from WordCountReducer's output dir
			statesList = fs.listStatus(new Path(args[1]+"/KeyWordsCount"));
			if(statesList != null){
			    for(FileStatus status : statesList){
			        //add each file to the list of inputs for the Map-Reduce job
			    	if (status.getLen() > 0) {
			    		FileInputFormat.addInputPath(cnfgRanking, status.getPath());
					} 
			    }
			}
			FileOutputFormat.setOutputPath(cnfgRanking, new Path(args[1]+"/Ranking"));
		      
			cnfgRanking.setMapperClass(RankingMapper.class);
			cnfgRanking.setReducerClass(RankingReducer.class);
			cnfgRanking.setInputFormat(KeyValueTextInputFormat.class);
			cnfgRanking.setMapOutputKeyClass(Text.class);
			cnfgRanking.setMapOutputValueClass(KeyWordsCatWritable.class);
			cnfgRanking.setOutputKeyClass(Text.class);
			cnfgRanking.setOutputValueClass(Text.class);
			cnfgRanking.set("mapred.textoutputformat.ignoreseparator", "true");  
			cnfgRanking.set("mapred.textoutputformat.separator", ",");//use comma as the separator
			job = JobClient.runJob(cnfgRanking);
			job.waitForCompletion();
		}
		*/
		//Ranking Map-Reduce: [<"keyword", "state">, <"count">] => top(sort_by_count([<"keyword">, <"state", "count">]),3)
				if (job.isSuccessful()) {
					JobConf cnfgDMList = new JobConf(WordsCountApp.class);
					fs = FileSystem.get(cnfgDMList); 
					//get FileStatus list from WordCountReducer's output dir
					statesList = fs.listStatus(new Path(args[1]+"/KeyWordsCount"));
					if(statesList != null){
					    for(FileStatus status : statesList){
					        //add each file to the list of inputs for the Map-Reduce job
					    	if (status.getLen() > 0) {
					    		FileInputFormat.addInputPath(cnfgDMList, status.getPath());
							} 
					    }
					}
					FileOutputFormat.setOutputPath(cnfgDMList, new Path(args[1]+"/DMList"));
				      
					cnfgDMList.setMapperClass(DominantListMapper.class);
					cnfgDMList.setReducerClass(DominantListReducer.class);
					cnfgDMList.setInputFormat(KeyValueTextInputFormat.class);
					cnfgDMList.setMapOutputKeyClass(Text.class);
					cnfgDMList.setMapOutputValueClass(Text.class);
					cnfgDMList.setOutputKeyClass(Text.class);
					cnfgDMList.setOutputValueClass(Text.class);
					cnfgDMList.set("mapred.textoutputformat.ignoreseparator", "true");  
					cnfgDMList.set("mapred.textoutputformat.separator", ",");//use comma as the separator
					job = JobClient.runJob(cnfgDMList);
					job.waitForCompletion();
				}
				//Ranking Map-Reduce: [<"keyword", "state">, <"count">] => top(sort_by_count([<"keyword">, <"state", "count">]),3)
				if (job.isSuccessful()) {
					JobConf cnfgDMCalcList = new JobConf(WordsCountApp.class);
					fs = FileSystem.get(cnfgDMCalcList); 
					//get FileStatus list from WordCountReducer's output dir
					statesList = fs.listStatus(new Path(args[1]+"/DMList"));
					if(statesList != null){
					    for(FileStatus status : statesList){
					        //add each file to the list of inputs for the Map-Reduce job
					    	if (status.getLen() > 0) {
					    		FileInputFormat.addInputPath(cnfgDMCalcList, status.getPath());
							} 
					    }
					}
					FileOutputFormat.setOutputPath(cnfgDMCalcList, new Path(args[1]+"/DMCalcList"));
				      
					cnfgDMCalcList.setMapperClass(DominantCalcMapper.class);
					cnfgDMCalcList.setReducerClass(DominantCalcReducer.class);
					cnfgDMCalcList.setInputFormat(KeyValueTextInputFormat.class);
					cnfgDMCalcList.setMapOutputKeyClass(Text.class);
					cnfgDMCalcList.setMapOutputValueClass(IntWritable.class);
					cnfgDMCalcList.setOutputKeyClass(Text.class);
					cnfgDMCalcList.setOutputValueClass(IntWritable.class);
					cnfgDMCalcList.set("mapred.textoutputformat.ignoreseparator", "true");  
					cnfgDMCalcList.set("mapred.textoutputformat.separator", ",");//use comma as the separator
					job = JobClient.runJob(cnfgDMCalcList);
					job.waitForCompletion();
				}
		//Ranking Map-Reduce: [<"keyword", "state">, <"count">] => top(sort_by_count([<"keyword">, <"state", "count">]),3)
		if (job.isSuccessful()) {
			JobConf cnfgDMRanking = new JobConf(WordsCountApp.class);
			fs = FileSystem.get(cnfgDMRanking); 
			//get FileStatus list from WordCountReducer's output dir
			statesList = fs.listStatus(new Path(args[1]+"/KeyWordsCount"));
			if(statesList != null){
			    for(FileStatus status : statesList){
			        //add each file to the list of inputs for the Map-Reduce job
			    	if (status.getLen() > 0) {
			    		FileInputFormat.addInputPath(cnfgDMRanking, status.getPath());
					} 
			    }
			}
			FileOutputFormat.setOutputPath(cnfgDMRanking, new Path(args[1]+"/DMRanking"));
		      
			cnfgDMRanking.setMapperClass(DominantListMapper.class);
			cnfgDMRanking.setReducerClass(DominantRankingReducer.class);
			cnfgDMRanking.setInputFormat(KeyValueTextInputFormat.class);
			cnfgDMRanking.setMapOutputKeyClass(Text.class);
			cnfgDMRanking.setMapOutputValueClass(Text.class);
			cnfgDMRanking.setOutputKeyClass(Text.class);
			cnfgDMRanking.setOutputValueClass(Text.class);
			cnfgDMRanking.set("mapred.textoutputformat.ignoreseparator", "true");  
			cnfgDMRanking.set("mapred.textoutputformat.separator", ",");//use comma as the separator
			job = JobClient.runJob(cnfgDMRanking);
			job.waitForCompletion();
		}
		
		//Ranking Map-Reduce: [<"keyword", "state">, <"count">] => top(sort_by_count([<"keyword">, <"state", "count">]),3)
				if (job.isSuccessful()) {
					JobConf cnfgDMRankingResult = new JobConf(WordsCountApp.class);
					fs = FileSystem.get(cnfgDMRankingResult); 
					//get FileStatus list from WordCountReducer's output dir
					statesList = fs.listStatus(new Path(args[1]+"/DMRanking"));
					if(statesList != null){
					    for(FileStatus status : statesList){
					        //add each file to the list of inputs for the Map-Reduce job
					    	if (status.getLen() > 0) {
					    		FileInputFormat.addInputPath(cnfgDMRankingResult, status.getPath());
							} 
					    }
					}
					FileOutputFormat.setOutputPath(cnfgDMRankingResult, new Path(args[1]+"/DMRankingResult"));
				      
					cnfgDMRankingResult.setMapperClass(DominantRankingMapper.class);
					cnfgDMRankingResult.setReducerClass(DominantRankingResult.class);
					cnfgDMRankingResult.setInputFormat(KeyValueTextInputFormat.class);
					cnfgDMRankingResult.setMapOutputKeyClass(Text.class);
					cnfgDMRankingResult.setMapOutputValueClass(Text.class);
					cnfgDMRankingResult.setOutputKeyClass(Text.class);
					cnfgDMRankingResult.setOutputValueClass(Text.class);
					cnfgDMRankingResult.set("mapred.textoutputformat.ignoreseparator", "true");  
					cnfgDMRankingResult.set("mapred.textoutputformat.separator", ",");//use comma as the separator
					job = JobClient.runJob(cnfgDMRankingResult);
					job.waitForCompletion();
				}
		return 0;
	}
}
