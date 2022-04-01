module org.apache.geode.common {
  requires com.fasterxml.jackson.databind;
  requires java.naming;
  requires static org.jetbrains.annotations;

  exports org.apache.geode.annotations;

  exports org.apache.geode.annotations.internal; // TODO to org.apache.geode.core;
  exports org.apache.geode.internal.lang; // TODO to org.apache.geode.core;
  exports org.apache.geode.internal.lang.utils; // TODO to org.apache.geode.core;
  exports org.apache.geode.util.internal; // TODO to org.apache.geode.core;
}
