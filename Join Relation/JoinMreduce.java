/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package joinmreduce;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.ArrayList;
import java.util.List;



/**
 *
 * @author srujan
 */
public class JoinMreduce {

    /**
     * @param args the command line arguments
     */
   
     
   public static void main(String[] args) {
        
      try{  
          Configuration conf= new Configuration();
         Job job = new Job(conf);
      
      String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
     if (otherArgs.length < 3) 
     {
       System.err.println("Usage: wordcount1.jar -input <path> -output <path> [-combiner] [-word-length x] [-prefix yyy]");
       System.exit(2);
     }
     
        String combiner="";
        int wordLength=-1;
        String prefix="";
        String input="";
        String input2="";
   
        String output="";
        for(int i=0;i<otherArgs.length;i++)
        {
           if(otherArgs[i].toString().equals("-input1"))
           {
           input=otherArgs[++i].toString();
           continue;
           }
           if(otherArgs[i].toString().equals("-input2"))
           {
           input2=otherArgs[++i].toString();
           continue;
           }
      
           if(otherArgs[i].toString().equals("-output"))
           {        
            output=otherArgs[++i].toString();
            continue;
           }
       
            System.err.println("Invalid Arguments; Usage: wordcount1.jar -input <path> -output <path> [-combiner] [-word-length x] [-prefix yyy]");
            System.exit(2);
        }
        
        job.setJarByClass(JoinMreduce.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        
        job.setReducerClass(ReduceJoin.class);

       
        job.setOutputFormatClass(TextOutputFormat.class);
      
            MultipleInputs.addInputPath(job, new Path(input), TextInputFormat.class, UserMap.class);
           MultipleInputs.addInputPath(job, new Path(input2), TextInputFormat.class, TweetMap.class);
           
             FileOutputFormat.setOutputPath(job, new Path(output)); 
              // MultipleInputs.   (job, new Path(args[1]), TextInputFormat.class, TweetMap.class);
         job.waitForCompletion(true);
      }
      
  
        catch(Exception e){
          e.printStackTrace();
        }
    
 
    }
    
    
    
    
    public static class UserMap extends Mapper<LongWritable, Text, Text, Text> 
    {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {  //login name state
            String line = value.toString().trim();
            if (line.equals("")) {
                return;
            }
            else {
                String[] s = line.split(",");
                //have used \t to seperate the columns since output format was not mentioned.
                context.write(new Text(s[0]), new Text("USER~" + s[1] + "," + s[2]));
                }
        }
    }

     public static class TweetMap extends Mapper<LongWritable, Text, Text, Text> 
     {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {  //tweet id, content , login
            String line = value.toString().trim();
            if (line.equals("")) 
            {
                return;
            }
            else
            {
                int lastIndex = line.lastIndexOf(",");
                int firstIndex = line.indexOf(",");
                context.write(new Text(line.substring(lastIndex + 1)), new Text("TWEET~"+line.substring(0, firstIndex) + "," + line.substring(firstIndex + 1, lastIndex)));
            }
        }
    }
    
    
    public static class ReduceJoin extends Reducer<Text, Text, Text, Text> 
    {
        private String user="";
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException 
        {
             List<String> tweets = new ArrayList<String>();
         
            for(Text currValue:values)
            {
             
             String valueSplitted[] = currValue.toString().split("~");
            
                 if(valueSplitted[0].equals("USER"))
                 {
                  // I have used \t to seperate the columns since it was not mentioned in the question which format the output should be.
                   user=key.toString()+","+valueSplitted[1].trim()+",";
                 }
                 else if(valueSplitted[0].equals("TWEET"))
                 {
                     tweets.add(valueSplitted[1].trim());
                 }
            }
            
      
          for (String rec: tweets) {
              
              context.write(new Text(user+rec),new Text(""));
    
                    }
              
         
        }
        
                
    }



     
}
