package com.matcha.test.mapper;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InterfaceAddress;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Matcha on 2016/10/25.
 */
public class OrderIntTestMapperTest
{
    @org.junit.Test
    public void map() throws Exception
    {
        URL baseURL = OrderIntTestMapperTest.class.getClassLoader().getResource("");
        URI baseURI = baseURL.toURI();
        URI fileURI = baseURI.resolve(new URI("../../src/main/resources/data/intList.txt"));
        URL fileURL = fileURI.toURL();
        try(
                InputStream inputStream = fileURL.openStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader)
        )
        {
            String str = null;
            Map<Integer, Integer> result = new HashMap<>(4);
            while((str = bufferedReader.readLine()) != null)
            {
                String[] information = str.split(" ");
                int outputKey = Integer.parseInt(information[0]);
                int outputValue = Integer.parseInt(information[1]);
                System.out.println(outputKey);
                System.out.println(outputValue);
                Integer value = result.get(outputKey);
                if(value == null)
                    result.put(outputKey, outputValue);
                else if(outputValue > value)
                    result.put(outputKey, outputValue);
            }
            System.out.println(result);
        }
    }

}