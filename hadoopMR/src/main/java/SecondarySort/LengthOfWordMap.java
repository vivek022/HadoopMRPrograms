package SecondarySort;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LengthOfWordMap extends Mapper<Object, Text, IntWritable, IntWritable>{
	private final static IntWritable one = new IntWritable(1);
	private IntWritable wordLength = new IntWritable();

	public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            wordLength.set(tokenizer.nextToken().length());
            context.write(wordLength, one);
        }
    }
}
	

  

