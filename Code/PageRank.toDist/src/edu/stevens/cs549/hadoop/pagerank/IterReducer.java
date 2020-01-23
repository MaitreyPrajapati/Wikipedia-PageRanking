package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double d = PageRankDriver.DECAY; // Decay factor
		/* 
		 * TODO: emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
		
		Iterator<Text> iter = values.iterator();
		
		double rank = 0; // default rank is 1 - d
		String adjacent_list = "";
		
		while(iter.hasNext()) {
			
			String line = iter.next().toString();
			
			if(!line.startsWith("Adjacent")) {
				rank += Double.valueOf(line);
			} else {
				adjacent_list = line.replaceAll("Adjacent", "");
			}
		}
		rank = 1 - d + rank * d;
		
		context.write(new Text(key + "+" + rank+"*"), new Text(adjacent_list));
	}
}
