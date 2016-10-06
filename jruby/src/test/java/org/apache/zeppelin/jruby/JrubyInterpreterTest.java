package org.apache.zeppelin.jruby;


import org.apache.log4j.BasicConfigurator;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/**
 * Tests for HBase Interpreter
 */
public class JrubyInterpreterTest {
  private static Logger logger = LoggerFactory.getLogger(JrubyInterpreterTest.class);
  private static JrubyInterpreter jrubyInterpreter;
  
  @BeforeClass
  public static void setUp() throws NullPointerException {
    BasicConfigurator.configure();
    Properties properties = new Properties();
    properties.put("jruby.com", "");
    properties.put("ruby.source", "");
    
    jrubyInterpreter = new JrubyInterpreter(properties);
    jrubyInterpreter.open();    
  }
  
  @Test
  public void putsTest() {
    InterpreterResult result = jrubyInterpreter.interpret("4 + 4", null);
    assertEquals(InterpreterResult.Code.SUCCESS, result.code());
    assertEquals(result.type(), InterpreterResult.Type.TEXT);
    assertEquals("Hello World\n", result.message());
  }

}
