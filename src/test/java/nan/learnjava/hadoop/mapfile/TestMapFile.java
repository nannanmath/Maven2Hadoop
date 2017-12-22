package nan.learnjava.hadoop.mapfile;

import java.io.IOException;

import org.apache.hadoop.conf.ConfigRedactor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TestMapFile {
	/**
	 * Write.
	 * @throws IOException 
	 */
	@Test
	public void write() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String name = "/usr/win7admin/mymap";
		
		Writer w = new Writer(conf, fs, name, IntWritable.class, Text.class);
		for (int i = 1; i < 5; ++i) {
			w.append(new IntWritable(i), new Text("tom" + i));
		}
		w.close();
		System.out.println("over!");
	}
	
	/**
	 * Read.
	 * @throws IOException 
	 */
	@Test
	public void readByKey() throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		String name = "/usr/win7admin/mymap";
		
		Reader r = new Reader(fs, name, conf);
		Text txt = new Text();
		Text retVal = (Text) r.get(new IntWritable(3), txt);
		System.out.println(retVal.toString());	
	}
}
