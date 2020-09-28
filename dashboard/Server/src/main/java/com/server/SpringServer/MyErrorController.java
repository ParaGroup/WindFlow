package com.server.SpringServer;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.boot.web.servlet.error.ErrorController;

// MyErrorController class
@Controller
public class MyErrorController implements ErrorController
{

    @RequestMapping("/error")
    public String handleError()
    {
        return "index.html";
    }

    @Override
    public String getErrorPath()
    {
        return "index.html";
    }
}
