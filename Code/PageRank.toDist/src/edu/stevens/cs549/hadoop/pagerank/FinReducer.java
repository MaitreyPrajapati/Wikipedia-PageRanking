package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> {

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		/* 
		 * TODO: For each value, emit: key:value, value:-rank
		 */
		Iterator<Text> iter = values.iterator();
		
		String node;
		
		while(iter.hasNext()) {
			
			node = iter.next().toString();

			System.out.println("###########################################################################");
			System.out.println("Inside FinReducer:");
			System.out.println(node+key.get());
			System.out.println("###########################################################################");
			
			context.write(new Text(node), new Text(String.valueOf(0 - key.get()))); //Converting -rank back to +rank 
		}
}
	}
