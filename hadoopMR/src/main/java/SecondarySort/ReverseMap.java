package SecondarySort;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class ReverseMap extends Mapper<Object, Text, Text,NullWritable>{
	NullWritable nw = NullWritable.get();
    private Text word = new Text();
    public void map(Object arg0, Text value,
    		Context context)
            throws IOException {
        String line = value.toString().concat("\n");
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            word.set(new StringBuffer(tokenizer.nextToken()).reverse().toString());
            
            try {
				context.write(word,nw);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
          }

    }

}

