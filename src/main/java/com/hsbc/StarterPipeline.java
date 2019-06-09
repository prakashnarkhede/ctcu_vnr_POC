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
package com.hsbc;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.codec.http.multipart.MemoryAttribute;

import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Instant;

/**
 * A starter example for writing Beam programs.
 *
 * <p>The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p> To run this starter example locally using DirectRunner, just
 * execute it without any additional parameters from your favorite development
 * environment.
 *
 * <p>To run this starter example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class StarterPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);
  public static void main(String[] args) throws Exception {
	  
	 //check about 	  --numWorkers  --> to set number of workers
	  Pipeline pipeline = Pipeline.create(
		        PipelineOptionsFactory.fromArgs(args).withValidation().create()); 
	 
	  //read text file in pipeline object
	  PCollection<String> fileData = pipeline.apply("Read input text file", TextIO.read().from("gs://ctct_vnr_bucket/*.txt"));
	  
	  //create process request function --> actual processing happens here
	  class ProcessRequest extends DoFn<String, String> {
	    @DoFn.ProcessElement
	    public void processElement(ProcessContext processContext) throws SQLException, Exception {
	    	processingLogic c = new processingLogic();
	    	System.out.println(c.jotrValidation(processContext.element()));
	    	processContext.output(c.jotrValidation(processContext.element()).toString());
	    }
	  }
	 
	  PCollection<String> numbersFromChars = fileData.apply(
	    ParDo.of(new ProcessRequest()));
	  
	  //write to output file
	  numbersFromChars.apply("WriteMyFile", TextIO.write().withoutSharding().to("Batch_Output_File").withSuffix(".txt"));

	  //run pipeline
	  pipeline.run().waitUntilFinish();

  }
}
