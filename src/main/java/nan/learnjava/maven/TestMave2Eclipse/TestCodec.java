package nan.learnjava.maven.TestMave2Eclipse;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.avro.file.BZip2Codec;
import org.apache.commons.compress.archivers.dump.DumpArchiveConstants.COMPRESSION_TYPE;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.DeflateCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;

public class TestCodec {

	public static void main(String[] args) {
		if (args == null || args.length == 0) {
			System.out.println("No arguments!");
			System.exit(-1);
		}
		File f = new File(args[0]);
		String srcPath = f.getAbsolutePath();
		new TestCodec().testBatchCompress(srcPath);
	}
	
	public void testBatchCompress(String srcPath) {
		Class[] codecs = {
				DefaultCodec.class,
				DeflateCodec.class,
				GzipCodec.class,
				BZip2Codec.class,
				Lz4Codec.class,
				SnappyCodec.class
		};
		for (Class codec : codecs) {
			try {
				testEncode(codec, srcPath);
				testDecode(codec, srcPath);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Test codec.
	 * @throws Exception 
	 */
	public void testEncode(Class codecClass, String srcPath) throws Exception {
		Class clazz = codecClass;
		Configuration conf = new Configuration();
		// Get instance.
		CompressionCodec codec = ReflectionUtils.newInstance(clazz, conf);
		String ext = codec.getDefaultExtension();
		long startTime = System.currentTimeMillis();
		// File input.
		FileInputStream fis = new FileInputStream(srcPath);
		// File output.
		FileOutputStream fos = new FileOutputStream(srcPath + ext);
		OutputStream os = codec.createOutputStream(fos); // Encode.
		// copy input to output.
		IOUtils.copyBytes(fis, os, 1024);
		os.close();
		long endTime = System.currentTimeMillis();
		File compf = new File(srcPath + ext);
		System.out.print(ext + ":\t" + "Comp. Time: " + (endTime - startTime) + "\t" + "Comp. Size: " + compf.length() + "\t");
	}
	
	/**
	 * Test Decode.
	 * @param enc
	 * @throws Exception
	 */
	public void testDecode(Class codecClass, String srcPath) throws Exception {
		Class clazz = codecClass;
		Configuration conf = new Configuration();
		// Get instance.
		CompressionCodec codec = ReflectionUtils.newInstance(clazz, conf);
		String ext = codec.getDefaultExtension();
		Decompressor decor = codec.createDecompressor();
		long startTime = System.currentTimeMillis();
		// File input.
		FileInputStream fis = new FileInputStream(srcPath + ext);
		// File output.
		FileOutputStream fos = new FileOutputStream(srcPath + ext + srcPath.substring(srcPath.lastIndexOf(".")));
		InputStream in = codec.createInputStream(fis, decor); // Decode.
		// copy input to output.
		IOUtils.copyBytes(in, fos, 1024);
		in.close();
		long endTime = System.currentTimeMillis();
		System.out.println("Decomp. Time: " + (endTime - startTime));
	}

}
