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

package org.opennms.core.ipc.sink.kafka;

import static com.jayway.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThan;

import java.util.ArrayList;
import java.util.Dictionary;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.camel.util.KeyValueHolder;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opennms.core.ipc.sink.api.MessageConsumer;
import org.opennms.core.ipc.sink.api.MessageProducer;
import org.opennms.core.ipc.sink.api.MessageProducerFactory;
import org.opennms.core.ipc.sink.api.SinkModule;
import org.opennms.core.ipc.sink.kafka.heartbeat.Heartbeat;
import org.opennms.core.ipc.sink.kafka.heartbeat.HeartbeatModule;
import org.opennms.core.test.OpenNMSJUnit4ClassRunner;
import org.opennms.minion.core.api.MinionIdentity;
import org.opennms.test.JUnitConfigurationEnvironment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.Timer.Context;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Used to help profile the sink producer and consumer
 * against Kafka.
 *
 * By default, we only run a quick test to validate the setup.
 *
 * A longer test, against which you can attach a profiler is available
 * but disabled by default.
 * 
 * @author ranger
 */
@RunWith(OpenNMSJUnit4ClassRunner.class)
@ContextConfiguration(locations={
        "classpath:/META-INF/opennms/applicationContext-soa.xml",
        "classpath:/META-INF/opennms/applicationContext-mockDao.xml",
        "classpath:/META-INF/opennms/applicationContext-proxy-snmp.xml",
        "classpath:/META-INF/opennms/applicationContext-ipc-sink-server-kafka.xml"
})
@JUnitConfigurationEnvironment
public class HeartbeatSinkPerfIT extends KafkaTestCase {

    private static final String REMOTE_LOCATION_NAME = "remote";

    @Autowired
    private KafkaMessageConsumerManager consumerManager;

    private List<HeartbeatGenerator> generators = new ArrayList<>();
    private final MetricRegistry metrics = new MetricRegistry();
    private final Meter receivedMeter = metrics.meter("receivedMeter");
    private final Meter sentMeter = metrics.meter("sent");
    private final Timer sendTimer = metrics.timer("send");

    // Tuneables
    private static final int NUM_CONSUMER_THREADS = 2;
    private static final int NUM_GENERATORS = 2;
    private static final double RATE_PER_GENERATOR = 1000.0;

    @SuppressWarnings( "rawtypes" )
    @Override
    protected void addServicesOnStartup(Map<String, KeyValueHolder<Object, Dictionary>> services) {
        services.put(MinionIdentity.class.getName(),
                     new KeyValueHolder<Object, Dictionary>(new MinionIdentity() {
                         @Override
                         public String getId() {
                             return "0";
                         }
                         @Override
                         public String getLocation() {
                             return REMOTE_LOCATION_NAME;
                         }
                     }, new Properties()));
    }

    @Override
    protected String getBlueprintDescriptor() {
        return "classpath:/OSGI-INF/blueprint/blueprint-ipc-client.xml";
    }

    @Override
    public boolean isCreateCamelContextPerClass() {
        return true;
    }

    @Override
    public void doPreSetup() throws Exception {
        super.doPreSetup();
        
        consumerManager.setKafkaAddress(System.getProperty("org.opennms.core.ipc.sink.kafka.kafkaAddress"));
        consumerManager.setZookeeperHost(System.getProperty("org.opennms.core.ipc.sink.kafka.zookeeperHost"));
        consumerManager.setZookeeperPort(Integer.getInteger("org.opennms.core.ipc.sink.kafka.zookeeperPort"));
        consumerManager.afterPropertiesSet();
    }

    public void configureGenerators() throws Exception {
        System.err.println("Starting Heartbeat generators.");

        /* Start the consumer */
        final HeartbeatModule parallelHeartbeatModule = new HeartbeatModule() {
            @Override
            public int getNumConsumerThreads() {
                return NUM_CONSUMER_THREADS;
            }
        };
        final HeartbeatConsumer consumer = new HeartbeatConsumer(parallelHeartbeatModule, receivedMeter);
        consumerManager.registerConsumer(consumer);

        /* Start the producer */
        final MessageProducerFactory remoteMessageProducerFactory = context.getRegistry().lookupByNameAndType("kafkaRemoteMessageProducerFactory", MessageProducerFactory.class);
        final MessageProducer<Heartbeat> producer = remoteMessageProducerFactory.getProducer(HeartbeatModule.INSTANCE);

        // Fire up the generators
        generators = new ArrayList<>(NUM_GENERATORS);
        for (int k = 0; k < NUM_GENERATORS; k++) {
            final HeartbeatGenerator generator = new HeartbeatGenerator(producer, RATE_PER_GENERATOR, sentMeter, sendTimer);
            generators.add(generator);
            generator.start();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (generators != null) {
            for (HeartbeatGenerator generator : generators) {
                generator.stop();
            }
            generators.clear();
        }
        consumerManager.unregisterAllConsumers();
        super.tearDown();
    }

    @Test(timeout=30000)
    public void quickRun() throws Exception {
        configureGenerators();
        await().until(() -> Long.valueOf(receivedMeter.getCount()), greaterThan(100L)); 
    }

    @Ignore
    public void longRun() throws Exception {
        // Here we enable console logging of the metrics we gather
        // To see these, you'll want to turn down the logging
        // You can do this by setting the following system property
        // on the JVM when running the tests:
        // -Dorg.opennms.core.test.mockLogger.defaultLogLevel=WARN

        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        try {
            reporter.start(15, TimeUnit.SECONDS);
            Thread.sleep(5 * 60 * 1000);
        } finally {
            reporter.stop();
        }
    }

    public static class HeartbeatConsumer implements MessageConsumer<Heartbeat> {

        private final HeartbeatModule heartbeatModule;
        private final Meter receivedMeter;

        public HeartbeatConsumer(HeartbeatModule heartbeatModule, Meter receivedMeter) {
            this.heartbeatModule = heartbeatModule;
            this.receivedMeter = receivedMeter;
        }

        @Override
        public SinkModule<Heartbeat> getModule() {
            return heartbeatModule;
        }

        @Override
        public void handleMessage(Heartbeat message) {
            receivedMeter.mark();
        }
    }

    public static class HeartbeatGenerator {
        Thread thread;

        final MessageProducer<Heartbeat> producer;
        final double rate;
        final AtomicBoolean stopped = new AtomicBoolean(false);
        private final Meter sentMeter;
        private final Timer sendTimer;

        public HeartbeatGenerator(MessageProducer<Heartbeat> producer, double rate) {
            this.producer = producer;
            this.rate = rate;
            MetricRegistry metrics = new MetricRegistry();
            this.sentMeter = metrics.meter("sent");
            this.sendTimer = metrics.timer("send");
        }

        public HeartbeatGenerator(MessageProducer<Heartbeat> producer, double rate, Meter sentMeter, Timer sendTimer) {
            this.producer = producer;
            this.rate = rate;
            this.sentMeter = sentMeter;
            this.sendTimer = sendTimer;
        }

        public synchronized void start() {
            stopped.set(false);
            final RateLimiter rateLimiter = RateLimiter.create(rate);
            thread = new Thread(new Runnable() {
                @Override
                public void run() {

                    while(!stopped.get()) {
                        rateLimiter.acquire();
                        try (Context ctx = sendTimer.time()) {
                            producer.send(new Heartbeat());
                            sentMeter.mark();
                        }
                    }
                }
            });
            thread.start();
        }

        public synchronized void stop() throws InterruptedException {
            stopped.set(true);
            if (thread != null) {
                thread.join();
                thread = null;
            }
        }
    }

    protected Long getCamelContextCreationTimeout() {
        return 1L;
    }
}
