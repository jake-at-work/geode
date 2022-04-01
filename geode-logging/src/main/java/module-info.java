module org.apache.geode.logging {
  requires transitive org.apache.logging.log4j;

  requires java.management;
  requires org.apache.geode.common;

  exports org.apache.geode.logging.internal.log4j.api; // TODO to org.apache.geode.core;
  exports org.apache.geode.logging.internal.executors; // TODO to org.apache.geode.core;
}