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

public class MovieTagMining {

    // Mapper
    public static class MovieMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value,
                        Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            // Skip empty lines or CSV header
            if (line.isEmpty()
                    || line.startsWith("\"movieId\"")
                    || line.startsWith("movieId")) {
                return;
            }

            // Handle CSV fields containing quotes
            String[] tokens =
                line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

            if (tokens.length >= 3) {

                String title =
                    tokens[1].replaceAll("^\"|\"$", "").trim();

                String rawGenres =
                    tokens[2].replaceAll("^\"|\"$", "").trim();

                // Convert | separated genres to readable format
                String tags = rawGenres.replace("|", ", ");

                context.write(
                    new Text(title),
                    new Text(tags)
                );
            }
        }
    }

    // Reducer
    public static class MovieReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key,
                            Iterable<Text> values,
                            Context context)
                throws IOException, InterruptedException {

            StringBuilder tagBuilder =
                new StringBuilder();

            boolean first = true;

            for (Text val : values) {

                if (!first) {
                    tagBuilder.append(", ");
                }

                tagBuilder.append(val.toString());
                first = false;
            }

            context.write(
                key,
                new Text(tagBuilder.toString())
            );
        }
    }

    // Driver
    public static void main(String[] args)
            throws Exception {

        if (args.length < 2) {

            System.err.println(
                "Usage: MovieTagMining " +
                "<input_path> <output_path>"
            );

            System.exit(1);
        }

        Configuration conf =
            new Configuration();

        Job job =
            Job.getInstance(
                conf,
                "Movie Tag Mining"
            );

        job.setJarByClass(
            MovieTagMining.class
        );

        job.setMapperClass(
            MovieMapper.class
        );

        job.setReducerClass(
            MovieReducer.class
        );

        job.setOutputKeyClass(
            Text.class
        );

        job.setOutputValueClass(
            Text.class
        );

        FileInputFormat.addInputPath(
            job,
            new Path(args[0])
        );

        FileOutputFormat.setOutputPath(
            job,
            new Path(args[1])
        );

        System.exit(
            job.waitForCompletion(true)
                ? 0 : 1
        );
    }
}