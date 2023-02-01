
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;

public class internet{
	// MApper class -> output -> string, int
	public static class internetMapper extends Mapper<Object, Text, Text, FloatWritable> {
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
    FloatWritable avg = new FloatWritable();	
	String line = value.toString();
        StringTokenizer st = new StringTokenizer(line,"\t");
        int sum = 0;
	String uname = "";
	uname = st.nextToken();
	while(st.hasMoreTokens()){
	sum = sum + Integer.parseInt(st.nextToken());
	}				
	avg.set((float)sum/7);
	context.write(new Text(uname), avg);	
     	}
     }  		
       // Reducer class -> string, int
       public static class internetReducer extends Reducer<Text,FloatWritable, Text, FloatWritable>{
       public void reduce(Text key,Iterable<FloatWritable> values,Context context)throws IOException, InterruptedException {
    float avg_val = 0f;

			for (FloatWritable num: values){
				avg_val = num.get();
				if(avg_val>5){
					context.write(key,new FloatWritable(avg_val));
				}
			}
		}
	}
  
public static void main(String args[]) throws Exception
{

// Create the object of configuration class
   Configuration conf = new Configuration();
  
// create the object of job class
   Job job = new Job(conf, "internet");
  
// set the data format of output
   job.setOutputKeyClass(Text.class);
   
// set the data type of output value
  job.setOutputValueClass(FloatWritable.class);
  
// set the data format of output
   job.setOutputFormatClass(TextOutputFormat.class);   

// set data format of input
   job.setInputFormatClass(TextInputFormat.class);
   
// set the name of wrapper class
   job.setMapperClass(internetMapper.class);
   
// set the name of Reducer class
   job.setReducerClass(internetReducer.class);
   
// set the input files path from oth argument
   FileInputFormat.addInputPath(job,new Path(args[0]));
   
// set the output files path from 1st argument
   FileOutputFormat.setOutputPath(job,new Path(args[1]));

// Execute the job and wait for completion
   job.waitForCompletion(true);
   
  }
}
