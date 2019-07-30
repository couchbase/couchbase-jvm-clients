package com.couchbase.client.test;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
public @interface IgnoreWhen {

  ClusterType[] clusterTypes() default {};

  Capabilities[] missesCapabilities() default {};

  Capabilities[] hasCapabilities() default {};

  int nodesLessThan() default 0;

  int nodesGreaterThan() default Integer.MAX_VALUE;

  int replicasLessThan() default 0;

  int replicasGreaterThan() default Integer.MAX_VALUE;

}
