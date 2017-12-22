package nan.learnjava.hadoop.sequencefile;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Sorter;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestSequenceFile {
	/**
	 * Write.
	 * @throws IOException 
	 */
	@Test
	public void write() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path name = new Path("/usr/win7admin/myseq100.seq");
		
		Writer w = SequenceFile.createWriter(fs, conf, name, IntWritable.class, Text.class);
		for (int i = 0; i < 100; ++i) {
			w.append(new IntWritable(i), new Text("tom" + i));
			//w.sync(); // Add sync. 
		}
		w.close();
		System.out.println("over!");
	}
	
	/**
	 * Write No order.
	 * @throws IOException 
	 */
	@Test
	public void writeNoOrder() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path name = new Path("/usr/win7admin/myseqNoOrder.seq");
		
		Writer w = SequenceFile.createWriter(fs, conf, name, IntWritable.class, Text.class);
		w.append(new IntWritable(1), new Text("tom" + 1));
		w.append(new IntWritable(5), new Text("tom" + 5));
		w.append(new IntWritable(3), new Text("tom" + 3));
		w.close();
		System.out.println("over!");
	}
	
	/**
	 * Sort a SequenceFile.
	 * @throws IOException 
	 */
	@Test
	public void sortSequenceFile() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path name = new Path("/usr/win7admin/myseqNoOrder.seq");
		Path sorted = new Path("/usr/win7admin/myseqOrder.seq");
		
		Sorter sorter = new Sorter(fs, IntWritable.class, Text.class, conf);
		sorter.sort(name, sorted);
	}
	
	/**
	 * Merge SequenceFiles.
	 * @throws IOException 
	 */
	@Test
	public void mergeSequenceFile() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path src1 = new Path("/usr/win7admin/myseqNoOrder.seq");
		Path src2 = new Path("/usr/win7admin/myseqOrder.seq");
		Path dest = new Path("/usr/win7admin/myseqMerge.seq");
		
		
		Sorter sorter = new Sorter(fs, IntWritable.class, Text.class, conf);
		sorter.merge(new Path[]{src1, src2}, dest);
	}
	
	/**
	 * Read
	 * @throws IOException 
	 */
	@Test
	public void read() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		//Path name = new Path("/usr/win7admin/myseq100.seq");
		//Path name = new Path("/usr/win7admin/myseqNoOrder.seq");
		//Path name = new Path("/usr/win7admin/myseqOrder.seq");
		Path name = new Path("/usr/win7admin/myseqMerge.seq");
		
		IntWritable k = new IntWritable();
		Text v = new Text();
		
		Reader r = new SequenceFile.Reader(fs, name, conf);
		while(true) {
			System.out.println(r.getPosition());
			boolean b = r.next(k, v);
			if (b) {
				System.out.println(k.get() + ":" + v.toString());
			} else {
				return;
			}
		}
	}
}
