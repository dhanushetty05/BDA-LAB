import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiplication {

    public static class MatrixMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        private int p;
        private int m;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            m = conf.getInt("m", 2);
            p = conf.getInt("p", 2);
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();

            if (line.isEmpty())
                return;

            String[] tokens = line.split(",");

            if (tokens.length != 4)
                return;

            String matrixName = tokens[0].trim();
            int row = Integer.parseInt(tokens[1].trim());
            int col = Integer.parseInt(tokens[2].trim());
            double val = Double.parseDouble(tokens[3].trim());

            Text outKey = new Text();
            Text outVal = new Text();

            if (matrixName.equalsIgnoreCase("A")) {

                for (int j = 0; j < p; j++) {
                    outKey.set(row + "," + j);
                    outVal.set("A," + col + "," + val);
                    context.write(outKey, outVal);
                }

            } else if (matrixName.equalsIgnoreCase("B")) {

                for (int i = 0; i < m; i++) {
                    outKey.set(i + "," + col);
                    outVal.set("B," + row + "," + val);
                    context.write(outKey, outVal);
                }
            }
        }
    }

    public static class MatrixReducer
            extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,
                            Context context)
                throws IOException, InterruptedException {

            HashMap<Integer, Double> hashA = new HashMap<>();
            HashMap<Integer, Double> hashB = new HashMap<>();

            for (Text val : values) {

                String[] parts = val.toString().split(",");

                String matrixName = parts[0].trim();
                int k = Integer.parseInt(parts[1].trim());
                double elementVal =
                        Double.parseDouble(parts[2].trim());

                if (matrixName.equals("A")) {
                    hashA.put(k, elementVal);
                } else if (matrixName.equals("B")) {
                    hashB.put(k, elementVal);
                }
            }

            double result = 0.0;

            for (int k : hashA.keySet()) {

                if (hashB.containsKey(k)) {
                    result += hashA.get(k) * hashB.get(k);
                }
            }

            context.write(key, new Text(String.valueOf(result)));
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 4) {
            System.err.println(
                "Usage: MatrixMultiplication " +
                "<input_path> <output_path> " +
                "<rows_of_A> <cols_of_B>"
            );
            System.exit(1);
        }

        Configuration conf = new Configuration();

        conf.setInt("m", Integer.parseInt(args[2]));
        conf.setInt("p", Integer.parseInt(args[3]));

        Job job = Job.getInstance(
                conf,
                "Matrix Multiplication"
        );

        job.setJarByClass(MatrixMultiplication.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

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