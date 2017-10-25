package nan.learnjava.maven.TestMave2Eclipse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class MvnHadoopTest {
	@Test
	public void listFS() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.201:8020/");
		FileSystem fs = FileSystem.get(conf);
		printPath(fs, new Path("/"));
	}

	/**
	 * Print path recursively.
	 */
	private void printPath(FileSystem fs, Path path) {
		System.out.println(path.toUri().toString());
		try {
			if(fs.isDirectory(path)) {
				FileStatus[] fsts = fs.listStatus(path);
				for (FileStatus fst : fsts) {
					Path p = fst.getPath();
					printPath(fs, p);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
