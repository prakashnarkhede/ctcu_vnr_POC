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

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class StarterPipeline {
	  private static final Logger LOG = LoggerFactory.getLogger(StarterPipeline.class);
	  public static void main(String[] args) throws Exception {

		 //check about 	  --numWorkers  --> to set number of workers
		  Pipeline pipeline = Pipeline.create(
			        PipelineOptionsFactory.fromArgs(args).withValidation().create()); 
		 
		  //read text file in pipeline object
		  PCollection<String> fileData = pipeline.apply("Read input text file", TextIO.read().from("gs://ctct_vnr_bucket/Consumer1/Batch_Input_File_C1.txt"));

		  //create process request function --> actual processing happens here
		  class ProcessRequest extends DoFn<String, String> {
		    @DoFn.ProcessElement
		    public void processElement(ProcessContext processContext) throws SQLException, Exception {
		    	processingLogic c = new processingLogic();
		    	System.out.println(c.jotrValidation(processContext.element()));
		    	processContext.output(c.jotrValidation(processContext.element()).toString());
		    }
		  }
		 
		  PCollection<String> processedData = fileData.apply(
		    ParDo.of(new ProcessRequest()));
	    
		  //write to output file
		  processedData.apply("WriteMyFile", TextIO.write().withoutSharding().to("Batch_Output_File").withSuffix(".txt"));

		  //run pipeline
		  pipeline.run().waitUntilFinish();
		  
		  //Delete input file once processed
			processingLogic c1 = new processingLogic();
	    	c1.DeleteFileFromGCS();

	  }
	}