package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\\*"); // Splits it into two parts. Part 1: node+rank | Part 2: Adjacent list

		if (sections.length > 2) // Checks if the data is in the incorrect format
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}
		
		/* 
		 * TODO: emit key: adjacent vertex, value: computed weight.
		 * 
		 * Remember to also emit the input adjacency list for this node!
		 * Put a marker on the string value to indicate it is an adjacency list.
		 */

		String[] noderank = sections[0].split("\\+"); // split node+rank
		
		String node = String.valueOf(noderank[0]);
		double rank = Double.valueOf(noderank[1]);
		
		String adjacent_list = sections[1].toString().trim(); //Adjacent List

		String[] adjacent_nodes = adjacent_list.split("\t",0);
		int len = adjacent_nodes.length; 
		
		//Calculating weight if curr page has outgoing links
		double curr_weight = ((double)1/len) * rank; 		
		
		for(String x : adjacent_nodes) {
			context.write(new Text(x), new Text(String.valueOf(curr_weight)));
		}
		
		//Writing with "Adjacent" so that it could be used for recognition/marker on the other end.
		context.write(new Text(node), new Text("Adjacent" + sections[1]));
	}

}
