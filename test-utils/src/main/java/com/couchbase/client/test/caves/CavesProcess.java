/*
 * Copyright (c) 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.client.test.caves;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.stream.Collectors;

public class CavesProcess {

  private final Path binPath;
  private final int controlPort;

  private volatile Thread logThread;
  private volatile Process cavesProcess;

  public CavesProcess(Path binPath, int controlPort) {
    this.binPath = binPath;
    this.controlPort = controlPort;
  }

  public void start() throws Exception  {
    cavesProcess = Runtime.getRuntime().exec(
      binPath.toAbsolutePath().toString()
        + " --control-port=" + controlPort
        + " --reporting-addr=127.0.0.1:9659");

    logThread = new Thread(() -> {
      try {
        while (true) {
          String text = new BufferedReader(
            new InputStreamReader(cavesProcess.getErrorStream(), StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));

           // System.err.println(text);

          text = new BufferedReader(
            new InputStreamReader(cavesProcess.getInputStream(), StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.joining("\n"));

           // System.err.println(text);

          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      } catch (Exception ex) {
        // System.err.println("logger thread terminated...");
      }
    });

    logThread.setDaemon(true);
    logThread.start();
  }

  public void stop() {
    cavesProcess.destroy();
  }
}
