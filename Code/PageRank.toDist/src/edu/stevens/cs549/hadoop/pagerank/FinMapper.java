package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		
		/*
		 * TODO output key:-rank, value: node
		 * See IterMapper for hints on parsing the output of IterReducer.
		 */
		
		//IterINPUT : KEY + RANK * LIST
		String[] arr = line.split("\\*"); 

		if (arr.length > 2) 
		{
			throw new IOException("Incorrect data format");
		}
			System.out.println("###########################################################################");

			System.out.println("Doesn't have two values !!!!!!");
			System.out.println(arr.length);
			System.out.println(arr[0]);
			
			System.out.println("###########################################################################");

		String[] Node_Rank = arr[0].split("\\+"); // KEY + RANK

		context.write(new DoubleWritable(0 - Double.valueOf(Node_Rank[1])), new Text(Node_Rank[0])); // Reverse shuffling the reducer by changing +rank to -rank
	}
}

