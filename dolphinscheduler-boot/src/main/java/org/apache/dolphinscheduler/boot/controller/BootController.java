package org.apache.dolphinscheduler.boot.controller;

import org.apache.dolphinscheduler.client.annotation.DolphinSchedulerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

/**
 * @Author: Tboy
 */
@Controller
public class BootController {

    private final Logger logger = LoggerFactory.getLogger(BootController.class);

    @DolphinSchedulerTask(name="demo", description = "test demo task")
    public void boot(){
        System.out.println("invoke boot method demo");
    }
}
