
package nan.learnjava.hadoop.writable;

public class Person {
	private String name;
	private int age;
	private boolean married;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public boolean isMarried() {
		return married;
	}
	public void setMarried(boolean married) {
		this.married = married;
	}
	public String toString() {
		return this.getName() + "," + this.getAge() + "," + this.isMarried();
	}
}
