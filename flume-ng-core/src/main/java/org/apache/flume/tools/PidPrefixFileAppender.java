package org.apache.flume.tools;

import org.apache.log4j.FileAppender;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

public class PidPrefixFileAppender extends FileAppender {
  @Override
  public void setFile(String file) {
    RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
    super.setFile(rt.getName() + "-" + file);
  }
}
