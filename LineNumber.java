import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.StringTokenizer;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
//<Word LineNumber>'s present
public class LineNumber {

    //Implementation of the mapper class
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
        //define two variables so that you can store the given user and their corresponding friends
        public Text String_Line = new Text();

        //counter to keep a track of the line number
        public IntWritable LineNumber = new IntWritable(0);



        //define the mapper function that produces out value pairs, based on input
        public void map (LongWritable Key, Text Value, Context context ) throws IOException,InterruptedException{


            //split Input data based on tab , as inputs are user ids and friends separated by a tab space
            String[] current_data = Value.toString().split(",");

            //check if the current_data is viable or not
            if(current_data.length == 0){
                return;
            }


            //Iterate through the Current_Data and produce the key value pairs, based on the integer values
            for (String data_point: current_data){

                //Set value for string_line
                String_Line.set(data_point.toString());

                //emit key value pair
                context.write(String_Line, LineNumber);

                //Increment_Line_Number
                LineNumber.set(LineNumber.get() + 1);
            }
        }
    }


    // Implementation of the reducer class
    public static class Reduce extends Reducer<Text, IntWritable, Text, Text>{


        //Define the reducer class
        public void reduce(Text key, Iterable<IntWritable>  Values, Context context) throws IOException,InterruptedException{

            //Store the collection of line numbers into an array
            ArrayList<Integer> values = new ArrayList<Integer>();


            for (IntWritable val : Values){
                values.add(val.get());
            }

            //Sort the list of Line Numbers based on their values
            Collections.sort(values);

            //Create new value of Type Text
            Text V = new Text(values.toString());

            //Set the value of the reducer at this stage
            context.write(key, V );

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
        Job job = new Job(conf, "LineNumber");
        job.setJarByClass(LineNumber.class);
        job.setMapperClass(LineNumber.Map.class);
        job.setReducerClass(LineNumber.Reduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //set output key type
        job.setOutputKeyClass(Text.class);
        //set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
