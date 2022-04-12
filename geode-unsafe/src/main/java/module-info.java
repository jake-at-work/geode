module org.apache.geode.unsafe {
  requires transitive java.management;
  requires jdk.unsupported;

  exports org.apache.geode.unsafe.internal.com.sun.jmx.remote.security;
}
