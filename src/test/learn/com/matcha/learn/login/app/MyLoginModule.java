package com.matcha.learn.login.app;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by Matcha on 2016/11/11.
 */
public class MyLoginModule implements LoginModule
{
    private final static Map<String, String> db;

    static
    {
        db = new HashMap<>(3);
        db.put("matcher", "matcher");
        db.put("admin", "admin");
        db.put("user", "user");
    }

    private Subject subject;
    private CallbackHandler callbackHandler;
    private Map<String, ?> sharedState;
    private Map<String, ?> options;
    private Principal entity;

    @Override
    public void initialize(Subject subject,
                           CallbackHandler callbackHandler,
                           Map<String, ?> sharedState,
                           Map<String, ?> options)
    {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.sharedState = sharedState;
        this.options = options;
        this.entity = null;
    }

    @Override
    public boolean login() throws LoginException
    {
        try
        {
            NameCallback nameCallback = new NameCallback("Please enter user name: ");
            PasswordCallback passwordCallback = new PasswordCallback("Please enter user password: ", false);
            Callback[] callbacks = new Callback[]{
                    nameCallback,
                    passwordCallback
            };
            callbackHandler.handle(callbacks);
            String userName = nameCallback.getName();
            char[] passWord = passwordCallback.getPassword();
            String shouldPassword = db.get(userName);
            if(shouldPassword == null)
                return false;
            else if(shouldPassword.equals(new String(passWord)))
            {
                this.entity = new MyPrincipal(userName);
                return true;
            }
            return false;
        }
        catch (IOException | UnsupportedCallbackException e)
        {
            e.printStackTrace();
            throw new LoginException("Login fail " + e.getMessage());
        }
    }

    @Override
    public boolean commit() throws LoginException
    {
        Set<Principal> principals = subject.getPrincipals();
        if(!principals.contains(entity))
            principals.add(entity);
        return true;
    }

    @Override
    public boolean abort() throws LoginException
    {
        Set<Principal> principals = subject.getPrincipals();
        principals.remove(entity);
        entity = null;
        return true;
    }

    @Override
    public boolean logout() throws LoginException
    {
        Set<Principal> principals = subject.getPrincipals();
        principals.remove(entity);
        entity = null;
        return true;
    }
}
