module org.apache.geode.serialization {
  requires org.apache.geode.common;
  requires org.apache.geode.logging;
  requires org.apache.commons.lang3;
  requires java.naming;
  requires java.management;
  requires java.rmi;

  requires static org.jetbrains.annotations;

  exports org.apache.geode.internal.serialization; // TODO to org.apache.geode.core;
}