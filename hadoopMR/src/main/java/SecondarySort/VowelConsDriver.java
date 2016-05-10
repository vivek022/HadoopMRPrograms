package SecondarySort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class VowelConsDriver {
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

	    Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf,
	    		args).getRemainingArgs();
	    		if (otherArgs.length != 2) {
	    		 System.err.println("Usage: WordCount <in> <out>");
	    		 System.exit(2);
	    		}
	    Job job = new Job(conf,"VowelConsDriver");

	    job.setJarByClass(VowelConsDriver.class);
	    job.setMapperClass(VowelConsMapper.class);
	    job.setReducerClass(VowelConsReducer.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);

	    // TODO: specify output types
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	    job.waitForCompletion(true);
	}

}
