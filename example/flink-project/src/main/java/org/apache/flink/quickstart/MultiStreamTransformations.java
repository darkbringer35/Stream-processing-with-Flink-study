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

import org.apache.flink.quickstart.data.Alert;
import org.apache.flink.quickstart.data.SmokeLevel;
import org.apache.flink.quickstart.data.SmokeLevelSource;
import org.apache.flink.quickstart.util.SensorReading;
import org.apache.flink.quickstart.util.SensorSource;
import org.apache.flink.quickstart.util.SensorTimeAssigner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * A simple application that outputs an alert whenever there is a high risk of fire.
 * The application receives the stream of temperature sensor readings and a stream of smoke level measurements.
 * When the temperature is over a given threshold and the smoke level is high, we emit a fire alert.
 */
public class MultiStreamTransformations {

	public static void main(String[] args) throws Exception {

		// set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// use event time for the application
		// 불필요해짐 env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// https://stackoverflow.com/questions/70962011/flink-timecharacteristic

		// configure watermark interval
		env.getConfig().setAutoWatermarkInterval(1000L);

		// ingest sensor stream
		DataStream<SensorReading> tempReadings = env
			// SensorSource generates random temperature readings
			.addSource(new SensorSource())
			// assign timestamps and watermarks which are required for event time
			.assignTimestampsAndWatermarks(new SensorTimeAssigner());

		// ingest smoke level stream
		DataStream<SmokeLevel> smokeReadings = env
			.addSource(new SmokeLevelSource())
			.setParallelism(1);

		// group sensor readings by sensor id
		KeyedStream<SensorReading, String> keyedTempReadings = tempReadings
			.keyBy(r -> r.id);

		// connect the two streams and raise an alert if the temperature and
		// smoke levels are high
		DataStream<Alert> alerts = keyedTempReadings
			.connect(smokeReadings.broadcast())
			.flatMap(new RaiseAlertFlatMap());

		alerts.print();

		// execute the application
		env.execute("Multi-Stream Transformations Example");
	}

	/**
	 * A CoFlatMapFunction that processes a stream of temperature readings ans a control stream
	 * of smoke level events. The control stream updates a shared variable with the current smoke level.
	 * For every event in the sensor stream, if the temperature reading is above 100 degrees
	 * and the smoke level is high, a "Risk of fire" alert is generated.
	 */
	public static class RaiseAlertFlatMap implements CoFlatMapFunction<SensorReading, SmokeLevel, Alert> {

		private SmokeLevel smokeLevel = SmokeLevel.LOW;

		@Override
		public void flatMap1(SensorReading tempReading, Collector<Alert> out) throws Exception {
			// high chance of fire => true
			if (this.smokeLevel == SmokeLevel.HIGH && tempReading.temperature > 100) {
				out.collect(new Alert("Risk of fire! " + tempReading, tempReading.timestamp));
			}
		}

		@Override
		public void flatMap2(SmokeLevel smokeLevel, Collector<Alert> out) {
			// update smoke level
			this.smokeLevel = smokeLevel;
		}
	}
}