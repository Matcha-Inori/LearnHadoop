package com.matcha.learn.hadoop.writable;

import com.matcha.learn.hadoop.writable.app.Person;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Matcha on 2016/11/17.
 */
public class PersonWritable implements WritableComparable<PersonWritable>
{
    private Text personName;
    private IntWritable personAge;

    public PersonWritable()
    {
        personName = new Text();
        personAge = new IntWritable();
    }

    public PersonWritable(Person person)
    {
        this(person.getName(), person.getAge());
    }

    public PersonWritable(String personName, int personAge)
    {
        this(new Text(personName), new IntWritable(personAge));
    }

    public PersonWritable(Text personName, IntWritable personAge)
    {
        this.personName = personName;
        this.personAge = personAge;
    }

    @Override
    public int compareTo(PersonWritable o)
    {
        return personAge.compareTo(o.personAge);
    }

    @Override
    public void write(DataOutput out) throws IOException
    {
        personName.write(out);
        personAge.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException
    {
        personName.readFields(in);
        personAge.readFields(in);
    }

    public static class PersonCompareable extends WritableComparator
    {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
        {
            
            return super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
}
