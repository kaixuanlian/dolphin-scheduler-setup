package org.apache.dolphinscheduler.client.registry;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @Author: Tboy
 */
@Component
public class DolphinSchedulerTaskRegistryConfig {

    @Value("${spring.application.name}")
    private String applicationName;

    @Value("${spring.group.name:'defaultGroup'}")
    private String groupName;

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }


}
