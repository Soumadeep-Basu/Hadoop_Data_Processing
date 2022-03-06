import java.io.IOException;
import java.security.Key;
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


//This is the Hadoop Based program's main class to read input data in from the mutual friends dataset and produce a list for all the
//mutual friends present
public class mutualFriends {

    //Implementation of the mapper class
    public static class Map extends Mapper<LongWritable, Text, Text, Text>{
        //define two variables so that you can store the given user and their corresponding friends
        private Text user = new Text();
        private Text friends = new Text();

        //define the mapper function that produces out value pairs, based on input
        public void map (LongWritable Key, Text Value, Context context ) throws IOException,InterruptedException{


            //split Input data based on tab , as inputs are user ids and friends separated by a tab space
            String[] current_data = Value.toString().split("\\t");

            //check if the current_data is viable or not
            if(current_data.length == 1){
                return;
            }

            //Store User-id
            String UserId = current_data[0];

            //store the FriendsList
            String[] Friends_List = current_data[1].split(",");

            //Iterate through the Friends_List and produce the key value pairs

            for (String Friends: Friends_List){

                //If the id equals userId, pass this iteration
                if(Friends.equals(UserId)){
                    continue;
                }

                //set User-Key based on numerical order to make sure there are no repeating values
                String UserKey;
                if (Integer.parseInt(UserId) < Integer.parseInt((Friends))){
                    UserKey = UserId + "," + Friends;
                }
                else{
                    UserKey = Friends + "," +  UserId ;
                }

                //Convert array into a readable format
                String regex="((\\b"+ Friends + "[^\\w]+)|\\b,?" + Friends + "$)";

                //set value for the key (tuple)
                friends.set(current_data[1].replaceAll(regex,""));

                //Set key
                user.set(UserKey);

                //emit key value pair
                context.write(user,friends);
            }
        }
    }


    // Implementation of the reducer class
    public static class Reduce extends Reducer<Text, Text, Text, Text>{

        //Function to see mutual or common friends based on two lists
        public String MatchingFriends(String First_Friend_List, String Second_Friend_List){

            //Base case when either of the two lists are null
            if(First_Friend_List == null || Second_Friend_List == null){
                return null;
            }

            String[] List1 = First_Friend_List.split(",");
            String[] List2 = Second_Friend_List.split(",");

            //Create two sets to calculate set difference

            //set1
            LinkedHashSet<String> firstset = new LinkedHashSet<>();
            for(String user:List1){
                firstset.add(user);
            }

            //set2
            LinkedHashSet<String> secondset = new LinkedHashSet<>();
            for(String user:List2){
                secondset.add(user);
            }

            //set difference to find all the mutual friends
            firstset.retainAll(secondset);
            return firstset.toString().replaceAll("\\[|\\]", "");

        }

        //Define the reducer class
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException{

            //Create List to store the two separate lists of strings
            String [] Friend_List = new String[10];
            int str_pointer = 0;

            //Separate the two strings
            for (Text value : values){
                Friend_List[str_pointer++] = value.toString();
            }

            //Get the matching values
            String Mutual_Friends = MatchingFriends(Friend_List[0], Friend_List[1]);

            //Convert Key to String
            String[] User_Key = key.toString().split(",");

            //Specify output pairs you want to check for
            if ( (User_Key[0].equals("0") && User_Key[1].equals("1")) || (User_Key[0].equals("20") && User_Key[1].equals("28193")) || (User_Key[0].equals("1") && User_Key[1].equals("29826")) || (User_Key[0].equals("6222") && User_Key[1].equals("19272")) || (User_Key[0].equals("28041") && User_Key[1].equals("28056"))) {
                if (Mutual_Friends != null && Mutual_Friends.length() != 0) {
                    context.write(key, new Text(Mutual_Friends));
                } else {
                    context.write(key, new Text("None"));
                }
            }
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
        //create a job with name "wordcount"
        Job job = new Job(conf, "mutualFriends");
        job.setJarByClass(mutualFriends.class);
        job.setMapperClass(mutualFriends.Map.class);
        job.setReducerClass(mutualFriends.Reduce.class);

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
