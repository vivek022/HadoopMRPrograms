package SecondarySort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LengthOfWordDriver {
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf,
	    		args).getRemainingArgs();
	    		if (otherArgs.length != 2) {
	    		 System.err.println("Usage: LengthOfWordDriver <in> <out>");
	    		 System.exit(2);
	    		}
	    Job job = new Job(conf,"LengthOfWordDriver");

	    job.setJarByClass(LengthOfWordDriver.class);
	    job.setMapperClass(LengthOfWordMap.class);
	    job.setReducerClass(LengthOfWordRed.class);

	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    // TODO: specify output types
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	    job.waitForCompletion(true);
	}

}


