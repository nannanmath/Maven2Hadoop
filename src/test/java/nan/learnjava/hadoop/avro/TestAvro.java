package nan.learnjava.hadoop.avro;

import java.io.File;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import Tutorial.Employee;

public class TestAvro {
	@Test
	public void testSerialize() throws IOException {
		Employee e1 = new Employee();
		e1.setName("Tom");
		e1.setAge(13);
		
		Employee e2 = Employee.newBuilder().setName("Jim").setAge(14).build();
		
		DatumWriter<Employee> dw = new SpecificDatumWriter<Employee>(Employee.class);
		DataFileWriter<Employee> fw = new DataFileWriter<Employee>(dw);
		fw.create(e1.getSchema(), new File("Employee.avro"));
		fw.append(e1);
		fw.append(e2);
		fw.close();
		System.out.println("Serialize over!");
	}
	
	@Test
	public void testDeserialize() throws IOException {
		DatumReader<Employee> dr = new SpecificDatumReader<Employee>(Employee.class);
		DataFileReader<Employee> fr = new DataFileReader<Employee>(new File("Employee.avro"), dr);
		Employee e = null;
		while (fr.hasNext()) {
			e = fr.next(e);
			System.out.println(e);
		}
	}
	
	@Test
	public void testNoComplieSerialize() throws IOException {
		Schema schema = new Schema.Parser().parse(new File("emp.avsc"));
		GenericRecord e1 = new GenericData.Record(schema);
		e1.put("Name", "XiaoMing");
		e1.put("age", 13);
		GenericRecord e2 = new GenericData.Record(schema);
		e2.put("Name", "LiLei");
		e2.put("age", 14);
		
		DatumWriter<GenericRecord> dw = new GenericDatumWriter<GenericRecord>(schema);
		DataFileWriter<GenericRecord> fw = new DataFileWriter<GenericRecord>(dw);
		fw.create(schema, new File("EmployeeNoComp.avro"));
		fw.append(e1);
		fw.append(e2);
		fw.close();
		
		System.out.println("over!");
	}
	 
	
	@Test
	public void TestNoCompileDeserilize() throws IOException {
		Schema schema = new Schema.Parser().parse(new File("emp.avsc"));		
		DatumReader<GenericRecord> dr = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> fr = new DataFileReader<GenericRecord>(new File("EmployeeNoComp.avro"), dr);
		GenericRecord e = null;
		while(fr.hasNext()) {
			e = fr.next(e);
			System.out.println(e);
		}
	}
	
}
