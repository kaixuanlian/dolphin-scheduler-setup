package org.apache.dolphinscheduler.client.annotation;

import java.lang.annotation.*;

/**
 * @Author: Tboy
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface DolphinSchedulerTask {

    String name() default "";

    String description() default "";
}
