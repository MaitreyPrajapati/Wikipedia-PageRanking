package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: Output key: node+rank, value: adjacency list
		 */
		
		int defualtrank = 1;
		Iterator<Text> iter = values.iterator();
		while(iter.hasNext()) {
			
			System.out.print(key);
			System.out.print(iter.toString());
			System.out.println();
			
			//Emitting Node+DefaultRank*AdjacentNodes
			context.write(new Text(key + "+" + defualtrank + "*"), iter.next());
			
		}
	}
}
