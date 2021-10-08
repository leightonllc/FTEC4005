import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MapReduceJoin {
    public static final String DELIMITER = "\t";

    public static class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {


        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {

            String filePath = ((FileSplit)context.getInputSplit()).getPath().toString();
            String line = value.toString();
            String[] values = line.split(DELIMITER);

            if (line == null || line.equals(""))
                return;

            if (filePath.contains("city")) {
                if (values.length < 2)
                    return;
                String id = values[0];
                String name = values[1];

                context.write(new Text(id), new Text("a#" + name));
            }

            else if (filePath.contains("stat")) {
                if (values.length < 3)
                    return;
                String id = values[0];
                String year = values[1];
                String num = values[2];

                context.write(new Text(id), new Text("b#" + year + DELIMITER + num));
            }
        }
    }


    public static class JoinReducer extends Reducer<Text,Text,Text,Text> {

        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            Vector<String> vecA = new Vector<String>();
            Vector<String> vecB = new Vector<String>();

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("a#")) {
                    vecA.add(value.substring(2));
                }
                else if (value.startsWith("b#")) {
                    vecB.add(value.substring(2));
                }
            }

            for (String u : vecA) {
                for (String w : vecB) {
                    context.write(key, new Text(u + DELIMITER + w));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Join");
        job.setJarByClass(MapReduceJoin.class);
        job.setMapperClass(JoinMapper.class);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
