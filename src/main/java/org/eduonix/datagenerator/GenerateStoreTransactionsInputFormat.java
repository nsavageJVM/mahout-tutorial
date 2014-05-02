package org.eduonix.datagenerator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.eduonix.datagenerator.TransactionIteratorFactory.KeyVal;
import org.eduonix.datagenerator.TransactionIteratorFactory.STATE;

/**
 * A simple input split that fakes input.
 */
public class GenerateStoreTransactionsInputFormat extends
		FileInputFormat<Text,Text> {

	@Override
	public RecordReader<Text, Text> createRecordReader(
			final InputSplit inputSplit, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
		return new RecordReader<Text, Text>() {
			
			@Override
			public void close() throws IOException {

			}
			
			/**
			 * We need the "state" information to generate records.
			 * - Each state has a probability associated with it, so that
			 * our data set can be realistic (i.e. Colorado should have more transactions
			 * than rhode island).
			 * 
			 * - Each state also will its name as part of the key.
			 * 
			 * - This task would be distributed, for example, into 50 nodes
			 * on a real cluster, each creating the data for a given state.
			 */
					
			//String storeCode = ((Split) inputSplit).storeCode;
			int records = ((StoreTransactionInputSplit) inputSplit).records;
			Iterator<KeyVal<String, String>> data = 
			        (new TransactionIteratorFactory(
			                records,((StoreTransactionInputSplit)inputSplit).state)).getData();
			KeyVal<String, String> currentRecord;

			@Override
			public Text getCurrentKey() throws IOException,
					InterruptedException {
				return new Text(currentRecord.key);
			}

			@Override
			public Text getCurrentValue() throws IOException,
					InterruptedException {
				return new Text(currentRecord.val);
			}

			@Override
			public void initialize(InputSplit arg0, TaskAttemptContext arg1)
					throws IOException, InterruptedException {
			}
			
			@Override
			public boolean nextKeyValue() throws IOException,
					InterruptedException {
				if(data.hasNext()) {
					currentRecord = data.next();
					return true;
				}
				return false;
			}

			@Override
			public float getProgress() throws IOException,
					InterruptedException {
				return 0f;
			}

		};
	}

    public enum props {

        store_records
    }
    
	@Override
	public List<InputSplit> getSplits(JobContext arg) throws IOException {
		int num_records_desired = arg.getConfiguration().
				getInt(GenerateStoreTransactionsInputFormat.props.store_records.name(),-1);
		if(num_records_desired == -1 ){
			throw new RuntimeException("# of total records not set in configuration object: " + 
		        arg.getConfiguration());
		}

		ArrayList<InputSplit> list = new ArrayList<InputSplit>();

		/**
		 * Generator class will take a state as input and generate all 
		 * the data for that state.
         * for each state in State
		 */
		for(TransactionIteratorFactory.STATE s : STATE.values())
		{
			StoreTransactionInputSplit split =
			        new StoreTransactionInputSplit(
			                (int)(Math.ceil(num_records_desired * s.probability)),s);
			System.out.println(s + " _ "+split.records);
			list.add(split);
		}
		return list;
	}

}