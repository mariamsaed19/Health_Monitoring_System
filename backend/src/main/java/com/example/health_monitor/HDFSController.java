package com.example.health_monitor;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@CrossOrigin(origins = "*")
@RestController
public class HDFSController {

    @GetMapping("/")
    public HealthAnalytics getWindow(@RequestParam(value = "from") String startDate,
                                     @RequestParam(value = "to") String endDate){
        return null;// MapReduce.bla bla..????
    }

}
