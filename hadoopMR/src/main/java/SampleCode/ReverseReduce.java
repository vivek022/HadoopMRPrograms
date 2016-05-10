package SampleCode;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReverseReduce extends
Reducer<Text, NullWritable, Text, NullWritable> {
    
    public void reduce(Text arg1,NullWritable val,
    		Context context)
            throws IOException {
        
        try {
			context.write(arg1,val);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

    }

}
