from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
conf = SparkConf().setAppName("SocketKafkaForwardConsumer").setMaster("local[2]")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10) # 2-second batch interval
lines = ssc.socketTextStream("localhost", 9999)
def process(rdd):
    count = rdd.count()
    if count > 0:
        print("Received {0} records in this batch".format(count))
        for i, record in enumerate(rdd.take(10), start=1):
            print("[{0}] {1}".format(i,record))
    else:
        print("No records in this batch")
lines.foreachRDD(process)
ssc.start()
ssc.awaitTermination()











 Step 1: Create Java File

Create a new file named WordCount.java and paste this code:

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    // Mapper Class
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    // Reducer Class
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    // Driver Code
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

⚙️ Step 2: Compile the Code

Run these commands in the terminal:

# Create directory for compiled classes
mkdir wordcount_classes

# Compile using Hadoop classpath
javac -classpath $(hadoop classpath) -d wordcount_classes WordCount.java

# Create JAR file
jar -cvf wordcount.jar -C wordcount_classes/ .

📁 Step 3: Prepare Input File in HDFS
# Create a sample input file
echo "Hadoop is a framework for distributed processing" > input.txt

# Create input directory in HDFS
hdfs dfs -mkdir /wordcountinput

# Upload input file to HDFS
hdfs dfs -put input.txt /wordcountinput/

🚀 Step 4: Run the MapReduce Job
hadoop jar wordcount.jar WordCount /wordcountinput /wordcountoutput


(Note: /wordcountoutput should not already exist — Hadoop requires a new output directory for each run.)

👀 Step 5: View the Output
hdfs dfs -cat /wordcountoutput/part-r-00000
