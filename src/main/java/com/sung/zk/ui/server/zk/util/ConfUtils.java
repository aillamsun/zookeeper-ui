package com.sung.zk.ui.server.zk.util;

import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

public class ConfUtils {
   private static final Logger LOGGER = LoggerFactory.getLogger(ConfUtils.class);
   private static final String CONF_PATH = "conf/conectionStrings.properties";

   public static Properties getConxtions() {
      Properties properties = null;
      try {
         properties = PropertiesLoaderUtils.loadProperties(new ClassPathResource(CONF_PATH));
      } catch (IOException e) {
         LOGGER.error(e.getMessage(), e);
      }

      return properties;
   }
}
