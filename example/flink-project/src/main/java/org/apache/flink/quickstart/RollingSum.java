/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.quickstart;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;

/**
 * Program demonstrating a rolling sum.
 */
public class RollingSum {

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple3<Integer, Integer, Integer>> inputStream = env.fromElements(
			Tuple3.of(1, 2, 2), Tuple3.of(2, 3, 1), Tuple3.of(2, 2, 4), Tuple3.of(1, 5, 3));

		DataStream<Tuple3<Integer, Integer, Integer>> resultStream = inputStream
			.keyBy(value -> value.f0) // key on first field of tuples, functional하게 명시해줘야 함
			.sum(1); // sum the second field of the tuple

		resultStream.print();

		// execute the application
		env.execute();
	}

}
