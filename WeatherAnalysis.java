import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherAnalysis {

    // Mapper Class
    public static class WeatherMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // Expected input format: Date,Temperature
            String line = value.toString().trim();

            if (line.isEmpty())
                return;

            String[] tokens = line.split(",");

            if (tokens.length == 2) {

                String date = tokens[0].trim();
                String tempStr = tokens[1].trim();

                // Emit Date as Key, Temperature as Value
                context.write(
                    new Text(date),
                    new Text(tempStr)
                );
            }
        }
    }

    // Reducer Class
    public static class WeatherReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,
                            Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {

                double temp =
                    Double.parseDouble(val.toString());

                String message;

                // Determine the weather condition
                if (temp > 35.0) {
                    message = temp +
                        " C -> Extremely Hot Day";

                } else if (temp >= 20.0) {
                    message = temp +
                        " C -> Pleasant / Warm Day";

                } else {
                    message = temp +
                        " C -> Cold / Chilly Day";
                }

                // Output: Date -> Message
                context.write(
                    key,
                    new Text(message)
                );
            }
        }
    }

    // Driver / Main Method
    public static void main(String[] args)
            throws Exception {

        if (args.length < 2) {
            System.err.println(
                "Usage: WeatherAnalysis " +
                "<input_path> <output_path>"
            );
            System.exit(1);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
            conf,
            "Weather Data Mining"
        );

        job.setJarByClass(WeatherAnalysis.class);

        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(
            job,
            new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job,
            new Path(args[1])
        );

        System.exit(
            job.waitForCompletion(true) ? 0 : 1
        );
    }
}