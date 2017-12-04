package nan.learnjava.maven.TestMave2Eclipse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import nan.learnjava.hadoop.writable.Person;
import nan.learnjava.hadoop.writable.PersonWritable;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class TestHadoopWritable {
	@Test
	public void testSerialize() throws Exception {
		// Hadoop serialize.
		IntWritable iw = new IntWritable();
		iw.set(100);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		iw.write(new DataOutputStream(baos));
		baos.close();
		byte[] data1 = baos.toByteArray();
		System.out.println("Hadoop serialize: " + data1.length);
		// Hadoop desrialize.
		iw.readFields(new DataInputStream(new ByteArrayInputStream(data1)));
		System.out.println("Hadoop deserialize: " + iw.get());
		
		// Java serialize.
		Integer i = new Integer(100);
		baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject(i);
		oos.close();
		byte[] data2 = baos.toByteArray();
		System.out.println("Java serialize: " + data2.length);
		// Java deserialize.
		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data2));
		Integer ii = (Integer)ois.readObject();
		System.out.println("Java deserialize: " + ii);
	}
	
	/**
	 * Serialize Array.
	 * @throws IOException
	 */
	@Test
	public void testArrayWritable() throws IOException {
		// Serialize.
		ArrayWritable aw = new ArrayWritable(Text.class);
		Text[] texts = {
				new Text("Hello"),
				new Text("World")
		};
		aw.set(texts);
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		aw.write(new DataOutputStream(baos));
		baos.close();
		// Deserialize.
		aw.readFields(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));
		Writable[] ws = aw.get();
		for (Writable w : ws) {
			Text t = (Text) w;
			System.out.println(t.toString());
		}
	}
	
	@Test
	public void testPersonWritable() {
		Person person = new Person();
		person.setName("Tom");
		person.setAge(22);
		person.setMarried(false);
		// Serialize
		PersonWritable pw = new PersonWritable();
		pw.setPerson(person);
		try {
			pw.write(new DataOutputStream(new FileOutputStream("Person.dat")));
		} catch (Exception e) {
			e.printStackTrace();
		}
		// Deserialize
		try {
			pw.readFields(new DataInputStream(new FileInputStream("Person.dat")));
			System.out.println(pw.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
