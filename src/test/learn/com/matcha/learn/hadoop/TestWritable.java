package com.matcha.learn.hadoop;

import org.apache.hadoop.io.Text;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

/**
 * Created by Matcha on 2016/11/11.
 */
public class TestWritable
{
    @Test
    public void testText()
    {
        Text text = new Text("hadoop");
        Assert.assertThat(text.getLength(), Is.is(6));
        Assert.assertThat(text.getBytes().length, Is.is(6));
        Assert.assertThat(text.charAt(2), Is.is((int) 'd'));
        Assert.assertThat("Out of bounds", text.charAt(100), Is.is(-1));

        Assert.assertThat("Find a substring ", text.find("do"), Is.is(2));
        Assert.assertThat("Finds first 'o' ", text.find("o"), Is.is(3));
        Assert.assertThat("Finds 'o' from position 4 or later", text.find("o", 4), Is.is(4));
        Assert.assertThat("No match", text.find("pig"), Is.is(-1));
    }

    @Test
    public void Text()
    {
        Text text = new Text("\u0041\u00DF\u6771\uD801\uDC00");
        Assert.assertThat(text.getLength(), Is.is(10));
        Assert.assertThat(text.find("\u0041"), Is.is(0));
        Assert.assertThat(text.find("\u00DF"), Is.is(1));
        Assert.assertThat(text.find("\u6771"), Is.is(3));
        Assert.assertThat(text.find("\uD801\uDC00"), Is.is(6));

        Assert.assertThat(text.charAt(0), Is.is(0x0041));
        Assert.assertThat(text.charAt(1), Is.is(0x00DF));
        Assert.assertThat(text.charAt(3), Is.is(0x6771));
        Assert.assertThat(text.charAt(6), Is.is(0x10400));
    }

    @Test
    public void string() throws UnsupportedEncodingException
    {
        String str = "\u0041\u00DF\u6771\uD801\uDC00";

        System.out.println("str is " + str);

        Charset charset = Charset.forName("UTF-8");
        Assert.assertThat(str.length(), Is.is(5));
        Assert.assertThat(str.getBytes(charset).length, Is.is(10));

        Assert.assertThat(str.indexOf("\u0041"), Is.is(0));
        Assert.assertThat(str.indexOf("\u00DF"), Is.is(1));
        Assert.assertThat(str.indexOf("\u6771"), Is.is(2));
        Assert.assertThat(str.indexOf("\uD801\uDC00"), Is.is(3));

        Assert.assertThat(str.charAt(0), Is.is('\u0041'));
        Assert.assertThat(str.charAt(1), Is.is('\u00DF'));
        Assert.assertThat(str.charAt(2), Is.is('\u6771'));
        Assert.assertThat(str.charAt(3), Is.is('\uD801'));
        Assert.assertThat(str.charAt(4), Is.is('\uDC00'));

        Assert.assertThat(str.codePointAt(0), Is.is(0x0041));
        Assert.assertThat(str.codePointAt(1), Is.is(0x00DF));
        Assert.assertThat(str.codePointAt(2), Is.is(0x6771));
        Assert.assertThat(str.codePointAt(3), Is.is(0x10400));
        Assert.assertThat(str.codePointAt(4), Is.is(0xDC00));

        System.out.println("code length - " + str.codePointCount(0, str.length()));

        System.out.println("===   charAt   ===");
        System.out.println(str.charAt(0));
        System.out.println(str.charAt(1));
        System.out.println(str.charAt(2));
        System.out.println(str.charAt(3));
        System.out.println(str.charAt(4));
        System.out.println("==================");

        System.out.println("===   codeAt   ===");
        System.out.println((char) str.codePointAt(0));
        System.out.println((char) str.codePointAt(1));
        System.out.println((char) str.codePointAt(2));
        System.out.println((char) str.codePointAt(3));
        System.out.println((char) str.codePointAt(4));
        System.out.println("==================");
    }

    @Test
    public void testChinese()
    {
        System.out.println();

        String str = "这是一串中文，并混合了english和标点！†˙¥\uD801\uDC00";
        Assert.assertThat("length ", str.length(), Is.is(27));

        System.out.println("===   charAt   ===");
        for(int i = 0;i < str.length();i++)
            System.out.println(str.charAt(i) + " " + (int) str.charAt(i));
        System.out.println("==================");

        System.out.println("===   codeAt   ===");
        for(int i = 0;i < str.length();i++)
            System.out.println((char) str.codePointAt(i) + " " + str.codePointAt(i));
        System.out.println("==================");
    }
}
