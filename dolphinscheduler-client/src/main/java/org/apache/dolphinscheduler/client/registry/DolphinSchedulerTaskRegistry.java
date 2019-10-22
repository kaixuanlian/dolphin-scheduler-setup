package org.apache.dolphinscheduler.client.registry;

import org.apache.dolphinscheduler.client.annotation.DolphinSchedulerTask;
import org.apache.dolphinscheduler.client.exceptions.TaskRegistryException;
import org.apache.dolphinscheduler.remote.utils.IPUtils;
import org.apache.zookeeper.KeeperException;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.scanners.TypeAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Author: Tboy
 */
@Component
public class DolphinSchedulerTaskRegistry implements ApplicationListener<ApplicationEvent> {

    private final Logger logger = LoggerFactory.getLogger(DolphinSchedulerTaskRegistry.class);

    @Autowired
    private DolphinSchedulerTaskRegistryConfig registerConfig;

    @Autowired
    private ZookeeperRegistryCenter zookeeperRegistryCenter;

    private final AtomicBoolean REGISTER = new AtomicBoolean(false);

    private final ConcurrentHashMap<String, DolphinSchedulerTaskRegistryBean> tasks = new ConcurrentHashMap<String, DolphinSchedulerTaskRegistryBean>();

    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        if (applicationEvent instanceof ContextRefreshedEvent && REGISTER.compareAndSet(false, true)) {
            try {
                Set<Method> annotationTask = this.findAnnotationTask();
                this.registerAnnotationTask(annotationTask);
            } catch (Throwable ex){
                logger.error("DolphinSchedulerTaskRegister error", ex);
            }
        } else{
            logger.info("DolphinSchedulerTaskRegister has registered");
        }
    }

    private Set<Method> findAnnotationTask(){
        ConfigurationBuilder builder = new ConfigurationBuilder().useParallelExecutor()
                .setUrls(ClasspathHelper.forClassLoader())
                .setScanners(new MethodAnnotationsScanner(), new TypeAnnotationsScanner());
        Reflections reflections = new Reflections(builder);
        Set<Method> methods = reflections.getMethodsAnnotatedWith(DolphinSchedulerTask.class);

        return methods;
    }

    private void registerAnnotationTask(Set<Method> methods){
        for(Method method : methods){
            Class<?> declaringClass = method.getDeclaringClass();
            String className = declaringClass.getName();
            String methodName = method.getName();
            Annotation[] annotations = method.getAnnotations();
            for (Annotation annotation : annotations) {
                if (annotation.annotationType().equals(DolphinSchedulerTask.class)) {
                    DolphinSchedulerTask task = (DolphinSchedulerTask) annotation;
                    String taskName = task.name();
                    String description = task.description();
                    DolphinSchedulerTaskRegistryBean registerBean = this.build(className, methodName, taskName, description);
                    this.registerTask(registerBean);
                }
            }
        }
    }
    private DolphinSchedulerTaskRegistryBean build(String className, String methodName, String taskName, String description){
        DolphinSchedulerTaskRegistryBean registerBean = new DolphinSchedulerTaskRegistryBean();
        registerBean.setApplicationName(registerConfig.getApplicationName());
        registerBean.setGroupName(registerConfig.getGroupName());
        registerBean.setTaskName(taskName);
        registerBean.setDescription(description);
        registerBean.setClassName(className);
        registerBean.setMethodName(methodName);
        return registerBean;
    }

    private void registerTask(DolphinSchedulerTaskRegistryBean registerBean){
        this.checkTaskName(registerBean);
        final String taskName = this.buildTaskName(registerBean);
        try {
            this.zookeeperRegistryCenter.persistTask(taskName, registerBean.toJson());
        } catch (KeeperException.NodeExistsException ex){
            //NOP
        } catch(Throwable ex){
            throw new RuntimeException(String.format("persist taskName %s fail", registerBean.getTaskName()));
        }
        try {
            this.zookeeperRegistryCenter.persistEphemeralWorker(taskName, IPUtils.getFirstNoLoopbackIP4Address());
        } catch(Throwable ex){
            throw new RuntimeException(String.format("persist taskName %s fail", registerBean.getTaskName()));
        }
    }

    private void checkTaskName(DolphinSchedulerTaskRegistryBean registerBean){
        final String taskName = this.buildTaskName(registerBean);
        final DolphinSchedulerTaskRegistryBean bean = this.tasks.putIfAbsent(taskName, registerBean);
        if(bean != null){
            throw new TaskRegistryException(String.format("duplicate taskName %s", registerBean.getTaskName()));
        }
//        final Set<String> taskNodes = this.zookeeperRegistryCenter.getTaskNodes();
//        if(taskNodes.contains(taskName)){
//            throw new TaskRegistryException(String.format("duplicate taskName %s in zookeeper", taskName));
//        }
    }

    private String buildTaskName(DolphinSchedulerTaskRegistryBean registerBean){
        StringBuilder builder = new StringBuilder(100);
        builder.append(registerBean.getApplicationName()).append("-");
        builder.append(registerBean.getGroupName()).append("-");
        builder.append(registerBean.getTaskName());
        return builder.toString();
    }
}
