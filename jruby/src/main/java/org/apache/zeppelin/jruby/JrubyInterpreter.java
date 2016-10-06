/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zeppelin.jruby;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.*;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.lang3.StringUtils;
import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.interpreter.thrift.InterpreterCompletion;
import org.apache.zeppelin.scheduler.Job;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jruby.embed.LocalContextScope;

/**
 * Jruby interpreter for Zeppelin.
 */
public class JrubyInterpreter extends Interpreter {
  Logger logger = LoggerFactory.getLogger(JrubyInterpreter.class);
  private ScriptingContainer scriptingContainer;
  private StringWriter writer;
 
  public JrubyInterpreter(Properties property) {
    super(property);
  }

  @Override
  public void open() {
    this.scriptingContainer  = new ScriptingContainer(LocalContextScope.SINGLETON);
    this.writer = new StringWriter();
    scriptingContainer.setOutput(this.writer);
  
  }

  @Override
  public void close() {
    if (this.scriptingContainer != null) {
      this.scriptingContainer.terminate();
    }
  }


  @Override
  public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {
    try {
      logger.info(cmd);
      this.writer.getBuffer().setLength(0);
      this.scriptingContainer.runScriptlet(cmd);
      this.writer.flush();
      logger.debug(writer.toString());
      return new InterpreterResult(InterpreterResult.Code.SUCCESS, writer.getBuffer().toString());
    } catch (Throwable t) {
      logger.error("Can not run '" + cmd + "'", t);
      return new InterpreterResult(InterpreterResult.Code.ERROR, t.getMessage());
    }
  }

  private Job getRunningJob(String paragraphId) {
    Job foundJob = null;
    Collection<Job> jobsRunning = getScheduler().getJobsRunning();
    for (Job job : jobsRunning) {
      if (job.getId().equals(paragraphId)) {
        foundJob = job;
      }
    }
    return foundJob;
  }

  @Override
  public void cancel(InterpreterContext context) {}
  
  @Override
  public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override
  public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public Scheduler getScheduler() {
    return SchedulerFactory.singleton().createOrGetParallelScheduler(
        JrubyInterpreter.class.getName() + this.hashCode(), 10);
  }

  @Override
  public List<InterpreterCompletion> completion(String buf, int cursor) {
    return null;
  }

}
