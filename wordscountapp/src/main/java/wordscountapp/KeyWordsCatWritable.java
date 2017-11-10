package wordscountapp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/*
 * Author: BICHENG XIAO
 * UCID: bx34
 * Email: bx34@njit.edu
 * */

public class KeyWordsCatWritable implements WritableComparable<KeyWordsCatWritable>{
	
	private Text state;
	private IntWritable count;
	
	public KeyWordsCatWritable() {
		this.state = new Text();
		this.count = new IntWritable();
	}
	
	//parameterized constructor
	public KeyWordsCatWritable(Text state,IntWritable count) {
		this.state = state;
		this.count = count;
	}
	
	public Text getState() {
		return state;
	}

	public void setState(Text state) {
		this.state = state;
	}

	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.state.readFields(arg0);
        this.count.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.state.write(arg0);
        this.count.write(arg0);
	}
	
	public int compareTo(KeyWordsCatWritable o) {
		if (o == null) return 0;
		int a = this.count.get();
		int b = o.getCount().get();
		System.out.println(a + " &&&& " + b);
		return   b < a ? -1:(b == a ? 0:1);
	}
	
	public String toString(){
		return this.state.toString() + "," + this.count;
	}

	public boolean equals(Object obj) {		
		if (obj == null) {
	        return false;
	    }
		
	    if (!KeyWordsCatWritable.class.isAssignableFrom(obj.getClass())) {
	        return false;
	    }
	    
	    KeyWordsCatWritable newObject = (KeyWordsCatWritable) obj;
	    
	    if ((this.count == null) || (newObject.count != null) || (this.state == null) || (newObject.state != null)) {
	        return false;
	    }
	    
		if (this.state.equals(newObject.state) && this.count.equals(newObject.count)) {
			return true;
		}
		return false;
	}
}

//Sorting class
class OrderByCount implements Comparator<Map.Entry<String, Integer>>
{
    public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2)
    {
    	 return (o2.getValue()).compareTo( o1.getValue());
    }
}

//Sorting class
class OrderByKeyword implements Comparator<Map.Entry<String, String>>
{
  public int compare(Map.Entry<String, String> o1, Map.Entry<String, String> o2)
  {
  	 return (o2.getKey()).compareTo(o1.getKey());
  }
}

