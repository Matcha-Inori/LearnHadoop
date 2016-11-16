package com.matcha.learn.login;

import com.matcha.learn.login.app.MyCallbackHandler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by Matcha on 2016/11/11.
 */
@RunWith(Parameterized.class)
public class TestLogin
{
    private String userName;
    private String passWord;

    public TestLogin(String userName, String passWord)
    {
        this.userName = userName;
        this.passWord = passWord;
    }

    @Test
    public void testLogin() throws LoginException, URISyntaxException
    {
        try
        {
            URL loginConfigURL = Thread.currentThread().getContextClassLoader()
                    .getResource("com/matcha/learn/login/app/login.conf");
            File loginConfig = new File(loginConfigURL.toURI());
            System.setProperty("java.security.auth.login.config", loginConfig.getAbsolutePath());
            CallbackHandler callbackHandler = new MyCallbackHandler(userName, passWord);
            LoginContext loginContext = new LoginContext("TestLogin", callbackHandler);
            loginContext.login();
        }
        catch (LoginException | URISyntaxException e)
        {
            e.printStackTrace();
            throw e;
        }
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data()
    {
        List<Object[]> params = new ArrayList<>();
        params.add(new Object[]{"admin", "admin"});
        params.add(new Object[]{"a", "123"});
        return params;
    }
}
