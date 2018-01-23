//import java.io.*;
//import org.apache.hadoop.io.*;
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

//import Transaction.MapClass;
//import Transaction.ReduceClass;


public class Movie1 {
public static class MapClass extends Mapper<LongWritable,Text,Text,Text>{
public void map(LongWritable key,Text value ,Context context)
{
		try{ 
			String a1="k";
        String[] str = value.toString().split(",");	
        if(str!=null&&str.length>0){
        String rating=str[3];
        String movie=str[1];
        double rating1=Double.parseDouble(rating);
        if(rating1>3.9)
        {	
        	String s4=Double.toString(rating1)+","+movie;
        context.write(new Text(a1), new Text(s4));
        
     }
	}
		}
     catch(Exception e)
     {
        System.out.println(e.getMessage());
     }
  }
}
public static class ReduceClass extends Reducer<Text,Text,Text,IntWritable>
{

	    public void reduce(Text key, Iterable<Text> values,Context context){
          try
          {
        	  
        	  /*String k1=key.toString();
          if(k1!=null&&k1.length()>0){
            Double rating=Double.parseDouble(k1);
            //int count=0;*/
            //if(rating>3.9)
            //{//
        	  int count=0;
        	  //String movie1="";
            	for(Text val:values)
            	{
            		String[] s2=val.toString().split(",");
                    double rating=Double.parseDouble(s2[0]);
                    //movie1=s2[1].toString();
                  if(rating>3.9){
                	  count+=1;  		
                  }
            	}
            	context.write(new Text(key), new IntWritable(count));
          }
            	//context.write(new DoubleWritable(rating),new IntWritable(count));
	    	/*int year1=Integer.valueOf(k1);
	    	if(year1>=1945&&year1<=1959)
            	
            {
            	int count=0;
	    	for(Text val: values)
              {
            	  String[] s1=val.toString().split(",");
              for(int i=0;i<s1.length;i++)
              {
            	  count++;
              }
              }
	    
	    context.write(new IntWritable(year1),new IntWritable(count));*/
	    catch(Exception e)
	    {
	    	System.out.println(e.getMessage());
	    }
	    
	    }
}
		  public static void main(String[] args) throws Exception {
			    Configuration conf = new Configuration();
			    //conf.set("name", "value")
			    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
			    Job job = Job.getInstance(conf, "Movie");
			    job.setJarByClass(Movie1.class);
			    job.setMapperClass(MapClass.class);
			    //job.setCombinerClass(ReduceClass.class);
			    job.setReducerClass(ReduceClass.class);
			    job.setNumReduceTasks(2);
			    job.setMapOutputKeyClass(Text.class);
			    job.setMapOutputValueClass(Text.class);
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(IntWritable.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}