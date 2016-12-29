package com.matcha.learn.test;

import com.matcha.learn.hadoop.writable.app.Person;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.UUID;

/**
 * Created by Matcha on 2016/11/9.
 */
@RunWith(Parameterized.class)
public class TestJUnit4
{
    private static String str = "default";
    private static ThreadLocal<UUID> threadId = new ThreadLocal<UUID>()
    {
        @Override
        protected UUID initialValue()
        {
            return UUID.randomUUID();
        }
    };

    private Person person1;
    private Person person2;
    private Person[] persons;

    public TestJUnit4(Person person1, Person person2, Person[] persons)
    {
        this.person1 = person1;
        this.person2 = person2;
        this.persons = persons;
    }

    @BeforeClass
    public static void beforeClass()
    {
        System.out.println("Thread - " + threadId.get() + " start! and str is " + str);
        str = "abcdefg";
    }

    @AfterClass
    public static void afterClass()
    {
        System.out.println("Thread - " + threadId.get() + " finish! and str is " + str);
    }

    @Test
    public void showPerson()
    {
        System.out.println("Thread - " + threadId.get() + " first person is " + person1);
        System.out.println("Thread - " + threadId.get() + " second person is " + person2);
        for(Person person : persons)
            System.out.println("Thread - " + threadId.get() + " persons - " + person);
    }

    @Parameterized.Parameters
    public static Object[][] provideParams()
    {
        return new Object[][]{
                {
                        new Person("AAA", 1),
                        new Person("BBB", 2),
                        new Person[]{
                                new Person("CCC", 3)
                        }
                },
                {
                        new Person("DDD", 4),
                        new Person("EEE", 5),
                        new Person[]{
                                new Person("FFF", 6),
                                new Person("GGG", 7)
                        }
                }
        };
    }
}