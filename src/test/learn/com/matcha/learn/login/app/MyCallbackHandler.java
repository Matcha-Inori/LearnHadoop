package com.matcha.learn.login.app;

import javax.security.auth.callback.*;
import java.io.IOException;

/**
 * Created by Matcha on 2016/11/11.
 */
public class MyCallbackHandler implements CallbackHandler
{
    private String userName;
    private String passWord;

    public MyCallbackHandler(String userName, String passWord)
    {
        this.userName = userName;
        this.passWord = passWord;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException
    {
        for(Callback callback : callbacks)
        {
            System.out.println(callback);
            if(NameCallback.class.isInstance(callback))
            {
                handleNameCallback((NameCallback) callback);
            }
            else if(PasswordCallback.class.isInstance(callback))
            {
                handlePassWordCallback((PasswordCallback) callback);
            }
            else
            {
                throw new UnsupportedCallbackException(callback);
            }
        }
    }

    private void handleNameCallback(NameCallback nameCallback)
    {
        nameCallback.setName(userName);
    }

    private void handlePassWordCallback(PasswordCallback passwordCallback)
    {
        passwordCallback.setPassword(passWord.toCharArray());
    }
}
