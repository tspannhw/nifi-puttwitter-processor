/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataflowdeveloper.processor.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author tspann
 *
 */
public class PutTwitterProcessorTest {

	private TestRunner testRunner;

	@Before
	public void init() {
		testRunner = TestRunners.newTestRunner(PutTwitterProcessor.class);
	}

	@Test
	public void testProcessor() {
//		try {
//			final Map<String, String> attributeMap = new HashMap<>();
//			attributeMap.put("field1", "f");
//			attributeMap.put("test1", "test1v");
//			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//			Date date = new Date();
//			testRunner.setProperty(PutTwitterProcessor.MESSAGE, "Current Time is " + dateFormat.format(date));
//			testRunner.setProperty(PutTwitterProcessor.LATITUDE, "40.268155");
//			testRunner.setProperty(PutTwitterProcessor.LONGITUDE, "-74.529094");
//			testRunner.enqueue(new FileInputStream(new File("src/test/resources/kafka.jpg")),
//			 attributeMap);
//
//			testRunner.setValidateExpressionUsage(false);
//			testRunner.run();
//			testRunner.assertValid();
//		} catch (Throwable e) {
//			e.printStackTrace();
//		}
//
//		List<MockFlowFile> successFiles = testRunner.getFlowFilesForRelationship(PutTwitterProcessor.REL_SUCCESS);
//
//		for (MockFlowFile mockFile : successFiles) {
//			try {
//				System.out.println("Returned" + mockFile.getSize());
//				
// 				mockFile.getAttributes().forEach((k, v) -> {
//					System.out.println("Result Key : " + k + "=" + v);
//				});
//			} catch (Throwable e) {
//				e.printStackTrace();
//			}
//		}
	}

}
