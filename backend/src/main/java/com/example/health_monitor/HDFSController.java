package com.example.health_monitor;
import com.example.QueryLayer.QueryHandler;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@CrossOrigin(origins = "*")
@RestController
public class HDFSController {

    @GetMapping("/")
    public List<String> getWindow(@RequestParam(value = "from") String startDate,
                                      @RequestParam(value = "to") String endDate) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("request received " + startDate + " " + endDate);
        // TODO change format to yyy-MM-dd-H_m
        DateFormat DFormat = new SimpleDateFormat("dd_MM_yyyy");
        Date start = null, end = null;
        try {
            start = DFormat.parse(startDate);
            end = DFormat.parse(endDate);
        } catch (ParseException e){
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Malformed dates");
        }
        // TODO pass request to query handler
        try {
            QueryHandler query = new QueryHandler();
            query.query(start.toString(),end.toString());
        } catch (SQLException e1) {
            e1.printStackTrace();
        }
        catch (IOException e2){
            //TODO try again
        }

        return null;
    }

}
