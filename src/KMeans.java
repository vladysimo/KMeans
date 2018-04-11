/*
 * @author Andrei Vlad Postoaca
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class KMeans {

	public static String CENTROID_FILE_NAME = "centroid.txt";
	public static String OUTPUT_FILE_NAME = "part-r-00000";
	public static String DATA_FILE_NAME = "data.txt";
	public static String JOB_NAME = "KMeans";
	public static String SPLITTER = "\t| ";
	public static List<Double> mCenters = new ArrayList<Double>();

	/*
	 * In Mapper class we are overriding configure function. In this we are
	 * reading file from Distributed Cache and then storing that into instance
	 * variable "mCenters"
	 */
	public static class KMeansMapper extends
			Mapper<LongWritable, Text, DoubleWritable, Text> {


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            super.setup(context);

            URI[] cacheFiles = context.getCacheFiles();
            System.out.println("=== " + cacheFiles.length + " " + cacheFiles[0].getPath() + " " +
                    cacheFiles[0].getPath().substring(cacheFiles[0].getPath().lastIndexOf("/") + 1));
            if (cacheFiles != null && cacheFiles.length > 0)
            {
                String line;
                mCenters.clear();
                BufferedReader cacheReader = new BufferedReader(new FileReader(cacheFiles[0].getPath().substring(cacheFiles[0].getPath().lastIndexOf("/") + 1)));
                    // Read the file split by the splitter and store it in
                    // the list
                while ((line = cacheReader.readLine()) != null) {
                    String[] temp = line.split(SPLITTER);
                    mCenters.add(Double.parseDouble(temp[0]));
                }
                System.out.println("=== " + mCenters);
                cacheReader.close();
            }
        }

        /*
		 * Map function will find the minimum center of the point and emit it to
		 * the reducer
		 */
		@Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            double point = Double.parseDouble(line);
            double min1, min2 = Double.MAX_VALUE, nearest_center = mCenters
                    .get(0);
            // Find the minimum center from a point
            for (double c : mCenters) {
                min1 = c - point;
                if (Math.abs(min1) < Math.abs(min2)) {
                    nearest_center = c;
                    min2 = min1;
                }
            }
            context.write(new DoubleWritable(nearest_center),
                    new Text(Double.toString(point)));
            System.out.println("=== [MAP] " + nearest_center + " " + point);
        }
    }

	public static class KMeansReducer extends
			Reducer<DoubleWritable, Text, DoubleWritable, Text> {

		/*
		 * Reduce function will emit all the points to that center and calculate
		 * the next center for these points
		 */
		@Override
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double newCenter;
			double sum = 0;
			int no_elements = 0;
			String points_out = "";

            System.out.print("=== [REDUCE] " + key + " ");

            for (Text val : values) {

			    String line = val.toString();
                String[] points_in = line.split(" ");

                for (String point : points_in)
                {
                    System.out.print(point + " ");
                    double d = Double.valueOf(point);
                    points_out = points_out + " " + point;
                    sum = sum + d;
                    no_elements++;
                }
            }
            System.out.println();

			// We have new center now
			newCenter = sum / no_elements;

			// Emit new center and point
			context.write(new DoubleWritable(newCenter), new Text(points_out));
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
			Job job = Job.getInstance(conf, "kmeans");

			if (iteration == 0) {
				//Path hdfsPath = new Path(input + CENTROID_FILE_NAME);
                Path hdfsPath = new Path("hdfs://localhost:9000/user/vlad/input/centroid.txt");
                System.out.println("=== " + hdfsPath.toUri());
				// upload the file to hdfs. Overwrite any existing copy.
				job.addCacheFile(hdfsPath.toUri());
			} else {
				//Path hdfsPath = new Path(again_input + OUTPUT_FILE_NAME);
                Path hdfsPath = new Path("output_" + (iteration - 1) + "/" + OUTPUT_FILE_NAME);
				// upload the file to hdfs. Overwrite any existing copy.
				job.addCacheFile(hdfsPath.toUri());
			}

            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            //job.setCombinerClass(KMeansReducer.class);
            job.setReducerClass(KMeansReducer.class);
            job.setMapOutputKeyClass(DoubleWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(DoubleWritable.class);
            job.setOutputValueClass(Text.class);

			/*
			FileInputFormat.setInputPaths(conf,
					new Path(input + DATA_FILE_NAME));
			FileOutputFormat.setOutputPath(conf, new Path(output));
			*/

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

            if (iteration == 5)
                break;


			/*
			// Sort the old centroid and new centroid and check for convergence
			// condition
			Collections.sort(centers_next);
			Collections.sort(centers_prev);

			Iterator<Double> it = centers_prev.iterator();
			for (double d : centers_next) {
				double temp = it.next();
				if (Math.abs(temp - d) <= 0.1) {
					isdone = true;
				} else {
					isdone = false;
					break;
				}
			}
			*/
			/*
			again_input = output;
			output = OUT + System.nanoTime();
			*/
		}
	}
}