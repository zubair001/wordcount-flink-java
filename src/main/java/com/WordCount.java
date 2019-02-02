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

package com;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram
 * over text files.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * Modified the program a bit.
 * * Isolated the basic wordcount example program from other programs. This is now standalone and awesome!
 * * Added csv file headers.
 * * Supports DataSet handling.
 * * Added additional logs.
 * *
 */
public class WordCount {

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text;
        if (params.has("input")) {
            // read the text file from given input path
            text = env.readTextFile(params.get("input"));

            System.out.println("Application/Log: Input file "+params.get("input")+" has been parsed.");

        } else {
            // get default test text data
            System.out.println("Application/Log: Executing WordCount example with default input data set.");
            System.out.println("Application/Log: Use --input to specify file input.");
            text = WordCountData.getDefaultTextLineDataSet(env);
        }

        DataSet<Tuple2<String,Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                text.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .groupBy(0)
                        .sum(1);

        /*
         Conversion to ArrayList structure.
          * Add the headers.
          * Ensure object data type is consistent.
         */
        ArrayList<Tuple2<String, String>> listWithHeaderInfo = new ArrayList<Tuple2<String,String>>();
        listWithHeaderInfo.add(new Tuple2<String, String>("Word", "Count"));

        System.out.println("Application/Log: Added headers.");

        System.out.println("Application/Log: Correcting data type inconsistency in the field - Count");



        for(Tuple2<String,Integer> tuplesOfWordAndCount:counts.collect()){

        	Tuple2<String,String> temporaryTuple = new Tuple2<String,String>();
        	temporaryTuple.f0 = tuplesOfWordAndCount.f0;
        	temporaryTuple.f1 = tuplesOfWordAndCount.f1.toString();

            listWithHeaderInfo.add(temporaryTuple);
        }

//        for (Iterator<Tuple2<String, Integer>> iter = counts.collect().iterator(); iter.hasNext(); ) {
//            Tuple2<String,Integer> element = iter.next();erty 6h5tcx
//
//            Tuple2<String,String> temporaryTuple = new Tuple2<String,String>();
//            temporaryTuple.f0 = element.f0;
//            temporaryTuple.f0 = element.f1.toString();
//            listWithHeaderInfo.add(temporaryTuple);
//
//        }

        System.out.println("Application/Log: Corrected data type consistency in the field - Count");

        //final dataset of words and counts to be used for printing.
        DataSet<Tuple2<String,String>> finalResultDataSet = env.fromCollection(listWithHeaderInfo);

        if (params.has("output")) {

            System.out.println("Application/Log: Writing output to file in CSV format.");

            finalResultDataSet.writeAsCsv(params.get("output"),"\n",",");

            System.out.println("Application/Log: Wrote to file: "+params.get("output"));

            // execute program
            env.execute("WordCount Example");

        } else {

            finalResultDataSet.print();

        }

    }

    /**
     * Implements the string tokenizer that splits sentences into words as a user-defined
     * FlatMapFunction. The function takes a line (String) and splits it into
     * multiple pairs in the form of "(word,1)" ({@code Tuple2<String, Integer>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));

                }
            }
        }
    }
}
