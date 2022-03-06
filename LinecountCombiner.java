import java.io.IOException;
import java.security.Key;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.StringTokenizer;

import com.sun.org.apache.xalan.internal.xsltc.dom.KeyIndex;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//This is the Hadoop Based program's main class to read input data in from the User Data dataset and produce a list for all the
//<Word LineNumber>'s present that have the maximum value
public class LinecountCombiner {

    //Implementation of the mapper class
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
        //Define the Key value, or variable that will store the value of the word that occurs the maximum number of times
        public Text key_index = new Text();

        //counter to keep track of maximum number of times a word occurs
        public IntWritable Occurance = new IntWritable(0);



        //define the mapper function that produces out value pairs, based on input
        public void map (LongWritable Key, Text Value, Context context ) throws IOException,InterruptedException{

            //split Input data based on tab , as inputs are user ids and friends separated by a tab space
            String[] current_data = Value.toString().split("\\t");

            //check if the current_data is viable or not
            if(current_data.length == 0){
                return;
            }

            //Set the value of the key
            key_index.set(current_data[0].toString());


            //Iterate through the Current_Data and set the value for the occurance

            StringBuilder S = new StringBuilder(current_data[1]);

            //Remove starting and ending []'s
            S.deleteCharAt(0);
            S.deleteCharAt(S.length() - 1);

            //set the length of occurance
            Occurance.set(S.toString().split(",").length);

            //Emit key value pairs
            context.write(key_index,Occurance);

        }
    }




    // Implementation of the Combiner Class
    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable>{

        //Variable that stores a value for maximum occurance
        public int max_count = 0;


        //Define the reducer class
        public void reduce(Text key, Iterable<IntWritable> Values, Context context) throws IOException,InterruptedException{

            for( IntWritable val : Values){

                //Check if occurance is greater than given value so that we can set occurance
                if(val.get() > max_count){
                    max_count = val.get();
                }

                //emit pairs that have value equal to max credit
                if(val.get() == max_count){
                    context.write(key, val);
                }
            }


        }


    }

    // Implementation of the reducer class
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{

        //Variable that stores a value for maximum occurance
        public int max_count =0;

        //Store the Maximum occurance of the word, and its value
        public String key_index;
        public int Counter;


        //Define the reducer class
        public void reduce(Text key, Iterable<IntWritable>  Values, Context context) throws IOException,InterruptedException{
            for( IntWritable val : Values){

                //Check if occurance is greater than given value so that we can set occurance
                if(val.get() > max_count){
                    max_count = val.get();
                }

                //emit pairs that have value equal to max credit
                if(val.get() == max_count){
                    Counter  = val.get();
                    key_index = key.toString();
                }
            }



        }

        @Override
        //Write Custom aggregate Function for the reducer class
        public void cleanup(Context context) throws IOException,InterruptedException{


            //Set the values for the maximum occuring word
            context.write(new Text(key_index), new IntWritable(Counter));

            super.cleanup(context);

        }


    }



    //Write the Driver (Main) Program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        //get all args
        if (otherArgs.length !=2) {
            System.err.println("Usage: WordCount <in> <out>, the given input is wrong");
            System.exit(2);
        }

        //create a job with name "Line Number"
        Job job = new Job(conf, "LinecountCombiner");
        job.setJarByClass(LinecountCombiner.class);
        job.setMapperClass(LinecountCombiner.Map.class);
        job.setCombinerClass(LinecountCombiner.Combiner.class);
        job.setReducerClass(LinecountCombiner.Reduce.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//
        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(IntWritable.class);

        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
