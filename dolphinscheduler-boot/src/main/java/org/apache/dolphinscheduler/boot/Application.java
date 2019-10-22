package org.apache.dolphinscheduler.boot;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

/**
 * @Author: Tboy
 */
@SpringBootApplication(scanBasePackages={"org.apache.dolphinscheduler"})
@ServletComponentScan
public class Application implements WebMvcConfigurer {

    public static void main(String[] args) {
        setUTF8Encoding();
        SpringApplication.run(Application.class, args);
    }

    private static void setUTF8Encoding() {
        System.setProperty("spring.http.encoding.force", "true");
        System.setProperty("spring.http.encoding.charset", "UTF-8");
        System.setProperty("spring.http.encoding.enabled", "true");
        System.setProperty("server.tomcat.uri-encoding", "UTF-8");
    }

}

