package nan.learnjava.maven.TestMave2Eclipse;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class MvnHadoopTest {

	@Test
	/**
	 * List all files and dirs.
	 */
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
			if (fs.isDirectory(path)) {
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

	/**
	 * Put file specify blocksize.
	 */
	@Test
	public void putFileWithBlocksize() {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.201:8020/");
		try {
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream fsdo = fs.create(new Path(
					"/usr/win7admin/blocksize1.txt"), true, 1024, (short) 2,
					1024);
			FileInputStream fis = new FileInputStream(
					"D:/HexoSourceCode/source/_posts/new1.md");
			IOUtils.copyBytes(fis, fsdo, 1024);
			fis.close();
			fsdo.close();
			System.out.println("over!");
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Get file status.
	 */
	@Test
	public void testFileStatus() {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.137.201:8020/");
		FileSystem fs = null;
		FileStatus status = null;
		try {
			fs = FileSystem.get(conf);
			status = fs
					.getFileStatus(new Path("/usr/win7admin/blocksize1.txt"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		Class clazz = FileStatus.class;
		Method[] ms = clazz.getMethods();

		// reflection
		for (Method m : ms) {
			try {
				String mname = m.getName();
				Class[] paramTypes = m.getParameterTypes();
				if ((mname.startsWith("get") || mname.startsWith("is"))
						&& (paramTypes == null || paramTypes.length == 0)) {
					Object ret = m.invoke(status);
					System.out.println(mname + ":" + ret);
				}
			} catch (Exception e) {
				e.printStackTrace();
				continue;
			}
		}
	}
	
	/**
	 * Test codec.
	 * @throws Exception 
	 */
	public void testEncode(Class codecClass) throws Exception {
		Class clazz = codecClass;
		Configuration conf = new Configuration();
		// Get instance.
		CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(clazz, conf);
		String ext = codec.getDefaultExtension();
		// File input.
		FileInputStream fis = new FileInputStream("x86.pdf");
		// File output.
		FileOutputStream fos = new FileOutputStream("x86.pdf" + ext);
		OutputStream os = codec.createOutputStream(fos); // Encode.
		// copy input to output.
		IOUtils.copyBytes(fis, os, 1024);
		os.close();
	}
	
	/**
	 * Test Decode.
	 * @param enc
	 * @throws Exception
	 */
	public void testDecode(Class codecClass) throws Exception {
		Class clazz = codecClass;
		Configuration conf = new Configuration();
		// Get instance.
		CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(clazz, conf);
		String ext = codec.getDefaultExtension();
		Decompressor decor = codec.createDecompressor();
		// File input.
		FileInputStream fis = new FileInputStream("x86.pdf" + ext);
		// File output.
		FileOutputStream fos = new FileOutputStream("x86.pdf" + ext + ".pdf");
		InputStream in = codec.createInputStream(fis, decor); // Decode.
		// copy input to output.
		IOUtils.copyBytes(in, fos, 1024);
		in.close();
	}
	
	@Test
	public void testCodec() throws Exception {
		//testEncode(GzipCodec.class);
		testDecode(GzipCodec.class);
	}
}
