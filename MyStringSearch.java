import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import java.util.StringTokenizer;
public class MyStringSearch {
public static class Tokenized Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	private final static IntWritable one=new IntWritable(1);
	public void map(LongWritable key,Text value,Context contetx){
		String my
	}
}

}
