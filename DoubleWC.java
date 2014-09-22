/* COUNT DOUBLE-WORD FREQUENCY
For example, "I am given an opportunity to use EC2 so I am very happy and very happy and very happy." The output would be something 	as follows (in any reasonable order):
I am 2
am given 1 AND SO ON..
*/

package org.myorg;
 	
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

 	
public class DoubleWC
{
	// MAPPER FUNCTION 	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>
	{
 	     private final static IntWritable one = new IntWritable(1);
 	     private Text word = new Text();
 	
 	     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException 		     {
		// GET A LINE FROM THE INPUT
 	     	String line = value.toString();
		
 	       	StringTokenizer tokenizer1 = new StringTokenizer(line);			// tokenizer1 WILL KEEP TRACK OF THE 1ST WORD	
		StringTokenizer tokenizer2 = new StringTokenizer(line);			// tokenizer2 WILL KEEP TRACK OF THE 2ND WORD

		// ADVANCE THE tokenizer2 TO POINT TO THE NEXT WORD IF THERE IS ANY SUCH WORD
		if (tokenizer2.hasMoreTokens())  tokenizer2.nextToken();

		/* WHILE tokenizer2 HAS MORE TOKENS/WORDS, GET EACH INTERMEDIATE OUTPUT AS ((<1ST WORD> <2ND WORD>), value) PAIRS
		   WHERE 1ST WORD IS BEING TRACKED BY tokenizer1 AND THE 2ND WORD IS TRACKED BY tokenizer2 */
 	       	while (tokenizer2.hasMoreTokens())
		{
		       	word.set(tokenizer1.nextToken() + " " +tokenizer2.nextToken());		// word = <1ST WORD> <2DN WORD>
	 	        output.collect(word, one);						// word IS GIVEN A COUNT OF 1
 	       	}
 	     }
	}
 	
 	
	// REDUCER FUNCTION
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
	{
 	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) 			throws IOException
	     {
 	     	int sum = 0;
		
		// SUM UP THE VALUES TO GET THE FINAL COUNT
 	       	while (values.hasNext())
 	        	 sum += values.next().get();
 	       
 	       	output.collect(key, new IntWritable(sum));
 	     }
 	}

 	
	public static void main(String[] args) throws Exception
	{
 	     JobConf conf = new JobConf(DoubleWC.class);
 	     conf.setJobName("DoubleWC");

 	     conf.setOutputKeyClass(Text.class);
 	     conf.setOutputValueClass(IntWritable.class);
 	
	     // SET THE MAPPER, COMBINE & REDUCER CLASSES
 	     conf.setMapperClass(Map.class);
 	     conf.setCombinerClass(Reduce.class);
 	     conf.setReducerClass(Reduce.class);
 	     
       	     // SPECIFY INPUT & OUTPUT FORMAT
 	     conf.setInputFormat(TextInputFormat.class);
 	     conf.setOutputFormat(TextOutputFormat.class);
 	
	     // SPECIFY INPUT & OUTPUT PATHS
 	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
 	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 	
 	     JobClient.runJob(conf);

	}
 }
