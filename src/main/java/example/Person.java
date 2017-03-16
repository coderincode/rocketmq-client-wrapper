package example;

import java.io.Serializable;
import java.util.List;

public class Person implements Serializable {

	private static final long serialVersionUID = -4165165210959448451L;

	private long id;
	private int age;
	private String name;
	private List<String> addrList;
	
	public Person(){}
	
	public Person(long id, int age, String name, List<String> addrList) {
		super();
		this.id = id;
		this.age = age;
		this.name = name;
		this.addrList = addrList;
	}
	
	public long getId() {
		return id;
	}
	public void setId(long id) {
		this.id = id;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public List<String> getAddrList() {
		return addrList;
	}
	public void setAddrList(List<String> addrList) {
		this.addrList = addrList;
	}
	
}
