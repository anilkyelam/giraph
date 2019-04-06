/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.giraph.io.formats;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.io.VertexInputFormat} for
 * unweighted graphs with long ids.
 *
 * Custom written for PageRank computation on unweighted graphs to suit SimplePageRankComputation in examples 
 * Initializes each vertex and edge with 0 values to start with
 * 
 * Each line consists of: vertex neighbor1 neighbor2 ... (seperated by spaces or commas)
 */
public class LongDoubleFloatTextInputFormat extends
    TextVertexInputFormat<LongWritable, DoubleWritable, FloatWritable> {
  /** Separator of the vertex and neighbors */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ,]");

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
                                             TaskAttemptContext context)
    throws IOException {
    return new LongLongNullVertexReader();
  }

  /**
   * Vertex reader associated with {@link LongLongNullTextInputFormat}.
   */
  public class LongLongNullVertexReader extends
      TextVertexReaderFromEachLineProcessed<String[]> {
        
    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      List<String> tokens = new ArrayList<String>();
      for(String token: SEPARATOR.split(line.toString())){
        if (!token.isEmpty()){
          tokens.add(token);
        }
      }

      return tokens.toArray(new String[tokens.size()]);
    }

    @Override
    protected LongWritable getId(String[] tokens) throws IOException {
      return new LongWritable(Long.parseLong(tokens[0]));
    }

    @Override
    protected DoubleWritable getValue(String[] tokens) throws IOException {
      return new DoubleWritable(0);
    }

    @Override
    protected Iterable<Edge<LongWritable, FloatWritable>> getEdges(
        String[] tokens) throws IOException {
      List<Edge<LongWritable, FloatWritable>> edges =
          Lists.newArrayListWithCapacity(tokens.length - 1);
      for (int n = 1; n < tokens.length; n++) {
        edges.add(EdgeFactory.create(
          new LongWritable(Long.parseLong(tokens[n])), 
          new FloatWritable(0)));
      }
      return edges;
    }
  }
}
