/**************************************************************************************
 *  Copyright (c) 2020- Gabriele Mencagli and Fausto Frasca
 *  
 *  This file is part of WindFlow.
 *  
 *  WindFlow is free software dual licensed under the GNU LGPL or MIT License.
 *  You can redistribute it and/or modify it under the terms of the
 *    * GNU Lesser General Public License as published by
 *      the Free Software Foundation, either version 3 of the License, or
 *      (at your option) any later version
 *    OR
 *    * MIT License: https://github.com/ParaGroup/WindFlow/blob/vers3.x/LICENSE.MIT
 *  
 *  WindFlow is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *  You should have received a copy of the GNU Lesser General Public License and
 *  the MIT License along with WindFlow. If not, see <http://www.gnu.org/licenses/>
 *  and <http://opensource.org/licenses/MIT/>.
 **************************************************************************************
 */

package com.server;

import java.io.IOException;
import java.net.BindException;
import com.server.CustomServer.*;
import javax.annotation.PreDestroy;
import com.server.ServerState.ServerState;
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
