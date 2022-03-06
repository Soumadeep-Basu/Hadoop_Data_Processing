import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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




//This is Hadoop Based code to get the Mutual Friends and their Birthdays, along with the number of birthdays that occur after 1995.
//This is done using a In Memory Combiner
public class InMemoryCombiner {


    //Create the Mapper Class
    public static class Map extends Mapper<LongWritable, Text,Text,Text>{

        //Create hash map to store user, DOB pairs in memory at the mapper side
        public HashMap<String,String> DOB = new HashMap<String,String>();

        //To store the user's that are being compared
        public Text User = new Text();

        //To store the value of the FriendsList
        public Text FriendsList = new Text();



        @Override
        //Read Initial Data (From userdata) and Initialize HashMap at the Reducer Side
        public void setup(Context context) throws InterruptedException,IOException{

            super.setup(context);

            //Get configuration for the files being issued as input
            Configuration conf = context.getConfiguration();

            //set file path to the input file
            Path fPath = new Path(conf.get("map.input"));
            FileSystem fs = FileSystem.get(conf);

            //Open the file for processing
            FileStatus[] status = fs.listStatus(fPath);
            for(FileStatus s : status) {
                BufferedReader br = new BufferedReader(
                        new InputStreamReader(fs.open(s.getPath())));
                String line;
                line = br.readLine();

                //Usecase for checking if line is empty or not
                while (line != null) {
                    String[] arr = line.split(",");
                    if (arr.length == 10)
                        DOB.put(arr[0], ":" + arr[9]);
                    line = br.readLine();
                }
            }
        }


        //Define class Mapper
        public void map (LongWritable Key, Text value, Context context) throws InterruptedException,IOException{
            String[] input = value.toString().split("\\t");

            //make sure that friends exits
            if (input.length < 2){
                return;
            }

            //the user ID for the friend
            String userId = input[0];

            //Potential list of Friends
            String[] friends = input[1].split(",");

            //Variable to store the DOB's
            String str = "";


            //Code to perform map-side join
            for (String friend : friends){

                str = str + DOB.get(friend) + ",";

            }

            //Convert DOB's into a string
            str = str.substring(0,str.length()-1);

           //Emit Input Output Pairs
            for (String friend: friends) {
                if (userId.equals(friend))
                    continue;
                // For consistency between two friends
                String userKey = (userId.compareTo(friend) < 0) ?
                        userId + "," + friend : friend + "," + userId;
                User.set(userKey);
                FriendsList.set(str);
                context.write(User, FriendsList);
            }


        }


    }


    //Define the Reducer Class
    public static class Reduce extends Reducer <Text, Text, Text, Text>{

        //Function to see the common friends or mutual friends based on the two lists
        private String mutualFriends(String list1, String list2) {
            if (list1 == null || list2 == null)
                return null;
            Set<String> set1 = new HashSet<>(Arrays.asList(list1.split(",")));
            Set<String> set2 = new HashSet<>(Arrays.asList(list2.split(",")));
            set1.retainAll(set2);
            //Return all the common friends
            return set1.toString();
        }



        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //To get the values of the specified users
            Configuration conf = context.getConfiguration();

            //Store the two users
            String U1 = conf.get("User1").toString();
            String U2 = conf.get("User2").toString();

            List<String> list = new ArrayList<>();
            for (Text value: values)
                list.add(value.toString());
            if (list.size() == 1)
                list.add("");

            //Divide the lists into the two incomming lists
            String mFriends = mutualFriends(list.get(0), list.get(1));
            String[] mFriendsList = mFriends.split(",");
            String mutualFriends = "";

            //To check for the number of mutual friends that have DOB's above 1995
            int counter = 0;
            for (String friend: mFriendsList){
                mutualFriends += friend.substring(friend.indexOf(':')+1) + ", ";
                //To see if the DOB is after 1995
                String[] dob = friend.substring(friend.indexOf(':')+1).split("/");

                if(dob.length == 3) {
                    StringBuilder S = new StringBuilder(dob[2]);
                    S.deleteCharAt(S.length() - 1);
                    if (dob[2].charAt(dob[2].length() - 1) == ']') {
                        dob[2] = S.toString();
                    }
                    if (Integer.parseInt(dob[2]) > 1995) {
                        counter = counter + 1;
                    }
                }


            }

            mutualFriends = mutualFriends.substring(0, mutualFriends.length()-2);
            String[] User_Values = key.toString().split(",");
            if(User_Values[0].equals(U1)&&User_Values[1].equals(U2) || User_Values[0].equals(U2)&&User_Values[1].equals(U1)) {

                if (mutualFriends.charAt(0) != '[') {
                    context.write(key, new Text("[" + mutualFriends + "" + Integer.toString(counter)));
                } else {
                    //To eliminate recurring brackets
                    context.write(key, new Text(mutualFriends + "" + Integer.toString(counter)));
                }

            }

        }

    }


   //Define the Driver Program
    public static void main(String[] args) throws Exception {

        //Get the configuration files
        Configuration conf = new Configuration();
        conf.set("map.input", args[1]);
        conf.set("User1",args[3]);
        conf.set("User2",args[4]);

        Job job = new Job(conf, "In Memory Combiner");
        job.setJarByClass(InMemoryCombiner.class);
        job.setMapperClass(InMemoryCombiner.Map.class);
        job.setReducerClass(InMemoryCombiner.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Get arguments from the other files
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
