package com.server;

import java.io.IOException;
import java.net.BindException;
import com.sun.tools.javac.Main;
import com.server.CustomServer.*;
import com.server.SpringServer.*;
import javax.annotation.PreDestroy;
import org.apache.commons.logging.Log;
import com.server.ServerState.ServerState;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

// MainClass class
@SpringBootApplication
public class MainClass extends SpringBootServletInitializer
{
    private static Class applicationClass = MainClass.class;
    private static Thread thread;
    private static ServerWF serverWF;

    // main method
    public static void main(String[] args)
    {
        ConfigurableApplicationContext springApplication = SpringApplication.run(MainClass.class,args); // server for the REST part        
        ServerState.getInstance();
        try {
            serverWF = new ServerWF();
            serverWF.configServer();
            thread = new Thread(serverWF); // internal server for receiving reports from WindFlow applications
            thread.start();
        }
        catch(BindException e){
            System.out.println("[Server] Socket port already used");
            SpringApplication.exit(springApplication, ()->0);
            return;
        }
        catch (IOException e) {
            e.printStackTrace();
            SpringApplication.exit(springApplication, ()->0);
            return;
        }
    }

    // shutdownCustomServer method
    @PreDestroy
    public void shutdownCustomServer()
    {
        serverWF.interrupt();
        try {
            thread.join(10000);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
