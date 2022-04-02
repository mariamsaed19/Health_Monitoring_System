package com.example.health_monitor;
import map_reduce.Analyser;
import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@CrossOrigin(origins = "*")
@RestController
public class HDFSController {

    @GetMapping("/")
    public List<JSONObject> getWindow(@RequestParam(value = "from") String startDate,
                                      @RequestParam(value = "to") String endDate) throws IOException, ClassNotFoundException, InterruptedException {
        DateFormat DFormat = new SimpleDateFormat("dd_MM_yyyy");
        Date start = null, end = null;
        try {
            start = DFormat.parse(startDate);
            end = DFormat.parse(endDate);
        } catch (ParseException e){
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Malformed dates");
        }
        Analyser analyser = new Analyser();
        if (analyser.generateStats(start.toString(), end.toString()))
            return analyser.readResults();
        return null;
    }

}
