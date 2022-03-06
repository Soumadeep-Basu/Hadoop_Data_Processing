import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.Period;
import java.util.*;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

//This is Hadoop Based code to get the minimum age of their direct friends.
//This is done using a In Memory Combiner
public class InMemoryReducer {

    //Write the Mapper Class
    public static class Map extends Mapper<LongWritable, Text, Text, Text>{

        //Define the Key
        public Text UserKey = new Text();

        //Define the Friends List
        public Text FriendsList = new Text();


        //Define the map function that produces key value pairs based on inputs
        public void map (LongWritable key, Text Value, Context context) throws IOException,InterruptedException{

            //Split the given string based on the tab space that is present
            String[] friends = Value.toString().split("\\t");

            //Eliminate if there are no friends
            if(friends.length < 2) {
                return;
            }

            //Set the Key
            UserKey.set(friends[0]);

            //Set the FriendsList
            FriendsList.set(friends[1]);


            //Emit key value paris

            context.write(UserKey,FriendsList);

        }
    }



    //Define the reducer class

    public static class Reduce extends Reducer<Text, Text, Text, Text> {


        //Create a Hashmap to store the information from the DB
        public HashMap<String, Integer> Age = new HashMap<>();

        //Create a setup function to perform initialization of the hashmap
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //set up config variable to retrieve config
            Configuration conf = context.getConfiguration();

            //get File path from config
            Path fPath = new Path(conf.get("reducer.input"));

            FileSystem fs = FileSystem.get(conf);
            FileStatus[] status = fs.listStatus(fPath);
            for (FileStatus s : status) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(s.getPath())));
                String line = br.readLine();
                while (line != null) {
                    String[] arr = line.split(",");
                    if (arr.length == 10)
                        Age.put(arr[0], getAge(arr[9]));
                    line = br.readLine();
                }


            }
        }


        //Define a function to get the age of the user
        public int getAge(String d) {
            String[] date = d.split("/");
            LocalDate currDate = LocalDate.now();
            LocalDate bDay = LocalDate.of(Integer.parseInt(date[2]), Integer.parseInt(date[0]), Integer.parseInt(date[1]));
            return Period.between(bDay, currDate).getYears();
        }


        //Define the reducer function
        public void reduce(Text key, Iterable<Text> Values, Context context) throws IOException,InterruptedException{

            //Variable to store min age
            Integer min_age = Integer.MAX_VALUE;

            for (Text val :Values){

                //String Array of all direct Friends
                String[] S = val.toString().split(",");

                for (String s : S){

                    if(Age.get(s)!=null){
                        min_age = Math.min(min_age,Age.get(s));
                    }
                }

            }


            //Produce Input Output Pairs

            context.write(key,new Text(min_age.toString()));


        }


    }
    //Define the Driver Program
    public static void main(String[] args) throws Exception {

        //Get the configuration files
        Configuration conf = new Configuration();
        conf.set("reducer.input", args[1]);

        Job job = new Job(conf, "In Memory Reducer");
        job.setJarByClass(InMemoryReducer.class);
        job.setMapperClass(InMemoryReducer.Map.class);
        job.setReducerClass(InMemoryReducer.Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Get arguments from the other files
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
