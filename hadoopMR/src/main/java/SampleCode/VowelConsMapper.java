package SampleCode;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class VowelConsMapper extends  Mapper<Object, Text, Text, IntWritable>{
	
	public void map(Object mapKey,Text mapValue,Context context) throws IOException, InterruptedException{

	    String line = mapValue.toString();
	    String[] letters = line.split("");

	    for(String letter : letters){
	        System.out.println(letter);
	        if(letter!=" "){
	            if(isVowel(letter))
	                context.write(new Text("Vowel"), new IntWritable(1));
	            else
	                context.write(new Text("Consonant"), new IntWritable(1));
	        }
	    }
	}

	private boolean isVowel(String letter) {
	    // TODO Auto-generated method stub
	    if(letter.equalsIgnoreCase("a")||letter.equalsIgnoreCase("e")||letter.equalsIgnoreCase("i")||letter.equalsIgnoreCase("o")||letter.equalsIgnoreCase("u"))
	        return true;
	    else
	        return false;
	}

}
