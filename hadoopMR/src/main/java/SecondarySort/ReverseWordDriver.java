package SecondarySort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ReverseWordDriver {
public static void main(String[] args) throws IOException, InterruptedException {
	 Configuration conf = new Configuration();
	    String[] otherArgs = new GenericOptionsParser(conf,
	    		args).getRemainingArgs();
	    		if (otherArgs.length != 2) {
	    		 System.err.println("Usage: ReverseWord <in> <out>");
	    		 System.exit(2);
	    		}
	    Job job = new Job(conf,"ReverseWord");

	    job.setJarByClass(ReverseWordDriver.class);
	    job.setMapperClass(ReverseMap.class);

	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(NullWritable.class);

	    // TODO: specify output types
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);

	    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	   			try {
					job.waitForCompletion(true);
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		

  }
}
