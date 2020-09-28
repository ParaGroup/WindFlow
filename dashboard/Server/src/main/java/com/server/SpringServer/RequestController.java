package com.server.SpringServer;

import com.google.gson.JsonArray;
import com.server.ServerState.ServerState;
import com.server.ServerState.ApplicationWF;
import org.springframework.web.bind.annotation.*;
import com.server.ServerState.SpringRequest.GeneralInformations;
import com.server.ServerState.SpringRequest.StatisticsApplicationWF;

// RequestController class
@RestController
public class RequestController
{
    ServerState serverState = ServerState.getInstance();

    @CrossOrigin(origins = "http://localhost:3000") // TODO to be deleted
    @GetMapping("/general")
    public GeneralInformations general()
    {
        return new GeneralInformations(this.serverState);
    }

    @CrossOrigin(origins = "http://localhost:3000") // TODO to be deleted
    @GetMapping("/get_all_app")
    public ApplicationWF getApplication(@RequestParam("id") Integer idApp)
    {
        return  serverState.getApp(idApp);
    }

    @CrossOrigin(origins = "http://localhost:3000") // TODO to be deleted
    @GetMapping("/get_stat_app")
    public StatisticsApplicationWF getStatApplication(@RequestParam("id") Integer idApp)
    {
        return  serverState.getStatApp(idApp);
    }

    @CrossOrigin(origins = "http://localhost:3000") // TODO to be deleted
    @GetMapping("/graph")
    public String getGraphWF(@RequestParam("id") Integer idApp)
    {
        return  serverState.getGraphWF(idApp);
    }

    @CrossOrigin(origins = "http://localhost:3000") // TODO to be deleted
    @GetMapping("/get_historical_data")
    public String getHistoricalData(@RequestParam("idApp") String idApp, @RequestParam("idOperator") String idOperator)
    {
        return  serverState.getStatOperator(idApp,idOperator);
    }
}
