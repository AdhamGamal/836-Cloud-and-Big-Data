package minStockPackage;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Created By: Mokhtar Adham, ID: 20398545
public class MinCloseStockReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	@Override
	protected void reduce(Text key, Iterable<FloatWritable> values,
			Reducer<Text, FloatWritable, Text, FloatWritable>.Context context) {

		try {

			float minStockClose = Float.MAX_VALUE;

			for (FloatWritable stockClose : values) {
				minStockClose = Math.min(minStockClose, stockClose.get());
			}

			context.write(key, new FloatWritable(minStockClose));

		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}

