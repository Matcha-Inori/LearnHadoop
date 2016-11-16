package com.matcha.learn.login.app;

import java.security.Principal;

/**
 * Created by Matcha on 2016/11/11.
 */
public class MyPrincipal implements Principal
{
    private String userName;

    public MyPrincipal(String userName)
    {
        this.userName = userName;
    }

    @Override
    public String getName()
    {
        return userName;
    }

    @Override
    public int hashCode()
    {
        return this.userName.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if(!this.getClass().isInstance(obj))
            return false;
        MyPrincipal otherPrincipal = (MyPrincipal) obj;
        if(userName == null)
            return otherPrincipal.userName == null;
        return userName.equals(otherPrincipal.userName);
    }

    @Override
    public String toString()
    {
        return "MyPrincipal{" +
                "userName='" + userName + '\'' +
                '}';
    }
}
