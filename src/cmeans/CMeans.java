package cmeans;/*
 * @author Andrei Vlad Postoaca
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;

@SuppressWarnings("deprecation")
public class CMeans {

    public static String CENTROID_FILE_NAME = "centroid.txt";
    public static String OUTPUT_FILE_NAME = "part-r-00000";
    public static String DATA_FILE_NAME = "data.txt";
    public static String JOB_NAME = "cmeans.CMeans";
    public static String SPLITTER = "\t| ";

    public static List<Double> mCenters = new ArrayList<>();
    //public static List<Map<Double, Double>> mDistances = new ArrayList<>();
    //public static List<Map<Double, Double>> mMembership = new ArrayList<>();
    public static final double FUZZINESS = 2d;

    /*
     * In Mapper class we are overriding configure function. In this we are
     * reading file from Distributed Cache and then storing that into instance
     * variable "mCenters"
     */
    public static class CMeansMapper extends
            Mapper<LongWritable, Text, IntWritable, Text> {


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            super.setup(context);

            URI[] cacheFiles = context.getCacheFiles();
            //System.out.println("=== " + cacheFiles.length + " " + cacheFiles[0].getPath() + " " +
            //        cacheFiles[0].getPath().substring(cacheFiles[0].getPath().lastIndexOf("/") + 1));
            if (cacheFiles != null && cacheFiles.length > 0)
            {
                String line;
                mCenters.clear();
                //mDistances.clear();
                //mMembership.clear();
                BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].getPath().substring(cacheFiles[0].getPath().lastIndexOf("/") + 1)));
                // Read the file split by the splitter and store it in
                // the list
                while ((line = cacheReader.readLine()) != null) {
                    String[] temp = line.split(SPLITTER);
                    mCenters.add(Double.parseDouble(temp[0]));
                    //mDistances.add(new HashMap<>());
                    //mMembership.add(new HashMap<>());
                }
                //System.out.println("=== " + mCenters);
                cacheReader.close();
            }
        }

        /*
         * Map function will find the minimum center of the point and emit it to
         * the reducer
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            double point = Double.parseDouble(value.toString());
            double distance, membership, numerator, denominator = 0, a, b, nearest_center = 0;
            double max = Double.MIN_VALUE;
            int nearest_center_id = 0;
            boolean is_centroid = false;

            List<Double> distances = new ArrayList(mCenters.size());
            List<Double> memberships = new ArrayList<>(mCenters.size());

            // Find the minimum center from a point
            for (int i = 0; i < mCenters.size(); i++) {
                distance = Math.abs(mCenters.get(i) - point);
                //mDistances.get(i).put(point, distance);
                distances.add(i, distance);
                if (distance == 0.0d)
                    is_centroid = true;
            }

            if (is_centroid) {

                for (int i = 0; i < mCenters.size(); i++)
                    if (mCenters.get(i) - point == 0) {
                        //mMembership.get(i).put(point, 1.0d);
                        memberships.add(i, 1.0d);
                        nearest_center_id = i;
                        nearest_center = mCenters.get(i);
                    }
                    else {
                        //mMembership.get(i).put(point, 0.0d);
                        memberships.add(i, 0.0d);
                    }
            }
            else {
                for (int i = 0; i < mCenters.size(); i++) {
                    //a = 1 / mDistances.get(i).get(point);
                    a = 1 / distances.get(i);
                    b = 1 / (FUZZINESS - 1);
                    denominator += Math.pow(a, b);
                }

                for (int i = 0; i < mCenters.size(); i++) {
                    //a = 1 / mDistances.get(i).get(point);
                    a = 1 / distances.get(i);
                    b = 1 / (FUZZINESS - 1);
                    numerator = Math.pow(a, b);
                    membership = numerator / denominator;
                    //mMembership.get(i).put(point, membership);
                    memberships.add(i, membership);

                    if (membership > max) {
                        nearest_center_id = i;
                        nearest_center = mCenters.get(i);
                        max = membership;
                    }
                }
            }

            for (int i = 0; i < mCenters.size(); i++) {
                context.write(new IntWritable(i), new Text(Double.toString(point) + " " + Double.toString(memberships.get(i))));
            }
            //System.out.println("=== [MAP] " + nearest_center_id + " " + nearest_center + " " + point);
        }
    }

    public static class CMeansReducer extends
            Reducer<IntWritable, Text, DoubleWritable, Text> {

        /*
         * Reduce function will emit all the points to that center and calculate
         * the next center for these points
         */
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double newCenter = -1, numerator = 0, denominator = 0, point, membership;
            String points_out = "";

            //System.out.print("=== [REDUCE] " + key + " ");

            List<Double> points = new ArrayList<>();
            List<Double> memberships = new ArrayList<>();

            for (Text val : values) {
                String[] pair = val.toString().split(" ");
                points.add(Double.valueOf(pair[0]));
                memberships.add(Double.valueOf(pair[1]));
            }

            for (int i = 0; i < points.size(); i++) {

                point = points.get(i);
                membership = memberships.get(i);
                //points_out = points_out + " " + point;
                //System.out.print(point + " ");

                numerator += Math.pow(membership, FUZZINESS) * point;
                denominator += Math.pow(membership, FUZZINESS);
            }
            //System.out.println();

            newCenter = numerator / denominator;

            context.write(new DoubleWritable(newCenter), new Text(""));
        }
    }

    public static void main(String[] args) throws Exception {
        run(args);
    }

    public static void run(String[] args) throws Exception {

        // Reiterating till the convergence

        int iteration = 0;
        boolean isdone = false;
        while (isdone == false) {
            System.out.println("=== " + iteration);
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "cmeans");

            if (iteration == 0) {
                //Path hdfsPath = new Path(input + CENTROID_FILE_NAME);
                Path hdfsPath = new Path("hdfs://localhost:9000/user/vlad/input/centroid.txt");
                //System.out.println("=== " + hdfsPath.toUri());
                // upload the file to hdfs. Overwrite any existing copy.
                job.addCacheFile(hdfsPath.toUri());
            } else {
                //Path hdfsPath = new Path(again_input + OUTPUT_FILE_NAME);
                Path hdfsPath = new Path("output_" + (iteration - 1) + "/" + OUTPUT_FILE_NAME);
                // upload the file to hdfs. Overwrite any existing copy.
                job.addCacheFile(hdfsPath.toUri());
            }

            job.setJarByClass(CMeans.class);
            job.setMapperClass(CMeansMapper.class);
            //job.setCombinerClass(CMeansReducer.class);
            job.setReducerClass(CMeansReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.setInputPaths(job, new Path("input/" + DATA_FILE_NAME));
            FileOutputFormat.setOutputPath(job, new Path("output_" + iteration + "/"));

            job.waitForCompletion(true);

            Path outfile = new Path("output_" + iteration + "/" + OUTPUT_FILE_NAME);
            FileSystem fs = FileSystem.get(job.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(
                    fs.open(outfile)));
            List<Double> centers_next = new ArrayList<>();
            String line = br.readLine();
            while (line != null) {
                String[] sp = line.split("\t| ");
                double c = Double.parseDouble(sp[0]);
                centers_next.add(c);
                line = br.readLine();
            }
            br.close();
            System.out.println("=== [NEXT_CENTERS] " + centers_next);

            String prev;
            if (iteration == 0) {
                prev = "input/" + CENTROID_FILE_NAME;
            } else {
                prev = "output_" + (iteration - 1) + "/" + OUTPUT_FILE_NAME;
            }

            Path prevfile = new Path(prev);
            FileSystem fs1 = FileSystem.get(job.getConfiguration());
            BufferedReader br1 = new BufferedReader(new InputStreamReader(
                    fs1.open(prevfile)));
            List<Double> centers_prev = new ArrayList<>();
            String l = br1.readLine();
            while (l != null) {
                String[] sp1 = l.split(SPLITTER);
                double d = Double.parseDouble(sp1[0]);
                centers_prev.add(d);
                l = br1.readLine();
            }
            br1.close();
            System.out.println("=== [PREV_CENTERS] " + centers_prev);

            iteration++;

            if (iteration == 10)
                break;
        }
    }
}