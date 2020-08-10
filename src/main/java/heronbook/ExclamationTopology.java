/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package heronbook;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.heron.api.utils.Utils;
import org.apache.heron.streamlet.Builder;
import org.apache.heron.streamlet.Config;
import org.apache.heron.streamlet.Runner;
import org.apache.heron.streamlet.Streamlet;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {

    public static void main(String[] args) throws Exception {
      String[] words = new String[] {
        "nathan", "mike", "jackson", "golda", "bertels"};

      Builder builder = Builder.newBuilder(); 

      Streamlet<String> s1 = builder.newSource(() -> {
        Utils.sleep(500);
        return words[ThreadLocalRandom
                       .current().nextInt(words.length)];
      }).setName("s1"); 
      Streamlet<String[]> s2 = builder.newSource(() -> {
        Utils.sleep(500);
        String word2 = 
          words[ThreadLocalRandom.current().nextInt(words.length)];
        String word3 = 
          words[ThreadLocalRandom.current().nextInt(words.length)];
        return new String[] {word2, word3};
      }).setName("s2"); 

      Streamlet<String> s3 = s1.map(x -> x+" !!!"); 
      Streamlet<String> s4 = s2.map(x -> x[0]+" & "+x[1]+" !!!");
      s3.union(s4).map(x -> x+" !!!").log(); 

      Config config = Config.newBuilder().setNumContainers(2)
                            .build(); 

      new Runner().run("my-java-streamlet", config, builder); 
    }

}
