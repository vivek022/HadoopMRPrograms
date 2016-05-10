package SecondarySort;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class VowelConsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	    public void reduce(Text letterType,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
	        int sum = 0;
	        for(IntWritable value:values){
	            sum += value.get();
	        }
	        System.out.println(letterType+"     "+sum);
	        context.write(letterType, new IntWritable(sum));
	    }
	}

