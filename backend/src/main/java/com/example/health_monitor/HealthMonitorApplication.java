package com.example.health_monitor;

import com.example.scheduler.Scheduler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class HealthMonitorApplication {

    public static void main(String[] args) {
            SpringApplication.run(HealthMonitorApplication.class, args);
            Thread thread = new Thread(Scheduler::main);
            thread.start();

    }

}
