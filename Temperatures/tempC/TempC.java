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


public class TempC {
 
    public static class TempCMapper extends Mapper <Object, /*Input key Type*/
    	    Text, /*Input value type*/
    	    Text, /*Output key Type*/
    	    FloatWritable  /*Output value type*/> {
           
    	// Map Function
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	 {
               
		String line[] = value.toString().split("\\s+");
	        float minTemp = Float.parseFloat(line[6]);
	        String year = line[1].substring(0,4);
	        if(minTemp > -60.0f && minTemp < 60.0f){
	           context.write(new Text(year), new FloatWritable(minTemp));
	        
		   }
    	      }
    	  }
    		 
    	          	// Reducer Class
    	public static class TempCReducer extends Reducer <Text, FloatWritable, Text, FloatWritable>{
                String year = null;
                float globalTemp = 100.0f;
    
        	public void reduce(Text key, Iterable <FloatWritable> values,
        	   		   Context context ) throws IOException, InterruptedException {
			float min = 100;
			for(FloatWritable temp : values){
			if(temp.get() < min)
			  min = temp.get();
			}
			if(min < globalTemp){
			globalTemp = min;
			year = key.toString();
			}
            		// context.write(key, new FloatWritable(max));
        	}
        	    // cleanup called once at the end of Mapper      
    	public void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(new Text("Coolest Year: " + year), new FloatWritable(globalTemp));  
    	}
      } 
       
       
    	public static void main(String args[]) throws Exception {
    
    		Configuration conf = new Configuration();
    		Job job = new Job(conf, "Temperature");
    		
    		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(FloatWritable.class);
    
                job.setOutputFormatClass(TextOutputFormat.class);
                job.setInputFormatClass(TextInputFormat.class);
                
    		job.setMapperClass(TempCMapper.class);
    		job.setReducerClass(TempCReducer.class);
    		
    		FileInputFormat.addInputPath(job, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Only 1 o\p path, inputs paths can be many.
    		job.waitForCompletion(true);
    
    	}
}
