module org.apache.geode.core {
  requires org.apache.geode.unsafe;
  requires org.apache.geode.common;
  requires org.apache.geode.logging;
  requires org.apache.geode.serialization;
  requires java.management;
  requires org.apache.commons.lang3;
  requires org.apache.logging.log4j;

  requires static org.jetbrains.annotations;
}
