//
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


public class Tempt {
 
    public static class TemptMapper extends Mapper <Object, /*Input key Type*/
    	    Text, /*Input value type*/
    	    Text, /*Output key Type*/
    	    FloatWritable  /*Output value type*/> {
           
    	// Map Function
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	 {
               
		String line[] = value.toString().split("\\s+");
	        float maxTemp = Float.parseFloat(line[5]);
	        String year = line[1].substring(0,4);
	        if(maxTemp > -60.0f && maxTemp < 60.0f){
	           context.write(new Text(year), new FloatWritable(maxTemp));
	        
		   }
    	      }
    	     /* // cleanup called once at the end of Mapper      
    	public void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(new Text(year), new FloatWritable(maxTemp));  
    	}
    	     } */ 	 
    	          	// Reducer Class
    	public static class TemptReducer extends Reducer <Text, FloatWritable, Text, FloatWritable>{
           
        	public void reduce(Text key, Iterable <FloatWritable> values,
        	   		   Context context ) throws IOException, InterruptedException {
			float max = -100;
			for(FloatWritable temp : values){
			if(temp.get() > max)
			  max = temp.get();
			}
            		 context.write(key, new FloatWritable(max));

        	}
        }
        
       
    	public static void main(String args[]) throws Exception {
    
    		Configuration conf = new Configuration();
    		Job job = new Job(conf, "Temperature");
    		
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(FloatWritable.class);
    
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setInputFormatClass(TextInputFormat.class);
                
    		job.setMapperClass(TemptMapper.class);
    		job.setReducerClass(TemptReducer.class);
    		
    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Only 1 o\p path, inputs paths can be many.
    		job.waitForCompletion(true);
    
    	}
}
