1. 启动manager， 模拟dolphin-scheduler中的MasterServer或者WEB-UI。
2. boot，模拟Java业务方项目, 引入dolphinscheduler-client依赖，然后在要调度的任务方法上，使用DolphinSchedulerTask注解配置任务，详见BootController。然后启动boot项目。
   ```
    <dependency>
     <groupId>org.apache.dolphinscheduler</groupId>
     <artifactId>dolphinscheduler-client</artifactId>
     <version>1.1.0-SNAPSHOT</version>
   </dependency>
   ```
3. boot启动后，会在zk下注册任务信息，manager发现任务信息后，会间隔10s进行业务方任务调度。
4. boot项目被调度后，会在控制台输出-invoke boot method demo
5. 以上仅简单演示基于注解式任务的调度方式，详细的任务编排需要在WEB-UI上操作。