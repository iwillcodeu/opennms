/*******************************************************************************
 * This file is part of OpenNMS(R).
 *
 * Copyright (C) 2016 The OpenNMS Group, Inc.
 * OpenNMS(R) is Copyright (C) 1999-2016 The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * OpenNMS(R) is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License,
 * or (at your option) any later version.
 *
 * OpenNMS(R) is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with OpenNMS(R).  If not, see:
 *      http://www.gnu.org/licenses/
 *
 * For more information contact:
 *     OpenNMS(R) Licensing <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 *******************************************************************************/

package org.opennms.core.test.kafka;

import java.io.File;
import java.net.ServerSocket;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;

import org.apache.commons.io.FileUtils;
import org.apache.curator.test.TestingServer;
import org.junit.rules.ExternalResource;

/**
 * This class starts up an embedded Kafka server for use in integration
 * tests.
 * 
 * @author Seth
 */
public class JUnitKafkaServer extends ExternalResource {

	private static KafkaConfig kafkaConfig;

	private KafkaServer kafkaServer;
	private TestingServer zkTestServer;

	private int kafkaPort;

	private int zookeeperPort;

	public int getKafkaPort() {
		return kafkaPort;
	}

	public int getZookeeperPort() {
		return zookeeperPort;
	}

	private static int getAvailablePort(int min, int max) {
		for (int i = min; i <= max; i++) {
			try (ServerSocket socket = new ServerSocket(i)) {
				return socket.getLocalPort();
			} catch (Throwable e) {}
		}
		throw new IllegalStateException("Can't find an available network port");
	}

	@Override
	public void before() throws Exception {
		// TODO: Access these values with methods
		zookeeperPort = getAvailablePort(2181, 2281);
		kafkaPort = getAvailablePort(9092, 9192);

		// Delete any existing Kafka log directory
		FileUtils.deleteDirectory(new File("target/kafka-log"));

		zkTestServer = new TestingServer(zookeeperPort);
		Properties properties = new Properties();
		properties.put("broker.id", "5001");
		properties.put("enable.zookeeper", "false");
		// Set this to only bind to localhost
		//properties.put("host.name", "localhost");
		properties.put("log.dir", "target/kafka-log");
		properties.put("port", String.valueOf(kafkaPort));
		properties.put("zookeeper.connect",zkTestServer.getConnectString());

		try {
			kafkaConfig = new KafkaConfig(properties);
			kafkaServer = new KafkaServer(kafkaConfig, null);
			kafkaServer.startup();
		} catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public void after() {
		kafkaServer.shutdown();
	}
}
