package nan.learnjava.hadoop.writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PersonWritable implements Writable{
	private Person person;
	
	public Person getPerson() {
		return person;
	}

	public void setPerson(Person person) {
		this.person = person;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(person.getName());
		out.writeInt(person.getAge());
		out.writeBoolean(person.isMarried());
	}

	public void readFields(DataInput in) throws IOException {
		person.setName(in.readUTF());
		person.setAge(in.readInt());
		person.setMarried(in.readBoolean());
	}
	
	public String toString() {
		return person.toString();
	}
}
