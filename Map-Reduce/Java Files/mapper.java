package minStockPackage;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Created By: Mokhtar Adham, ID: 20398545
public class MinCloseStockMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, FloatWritable>.Context context) {

		try {

			String record = value.toString();
			String[] stocks = record.split(",");
			String ticker = stocks[0];
			float stockClose = Float.parseFloat(stocks[5]);

			context.write(new Text(ticker), new FloatWritable(stockClose));

		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}