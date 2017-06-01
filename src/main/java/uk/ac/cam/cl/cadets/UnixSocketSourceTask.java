/*-
 * Copyright (c) 2017 (Graeme Jenkinson)
 * All rights reserved.
 *
 * This software was developed by BAE Systems, the University of Cambridge
 * Computer Laboratory, and Memorial University under DARPA/AFRL contract
 * FA8650-15-C-7558 ("CADETS"), as part of the DARPA Transparent Computing
 * (TC) research program.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 */

package uk.ac.cam.cl.cadets;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * UnixSocketSourceTask sends messages recived from the Unix domain socket to 
 * the Kafka topic.
 *
 * The UnixSocketSource polls the queue held by the UnixSocketServerThread
 * for new messages. Where possible the messages are batched in batches of
 * batchSize (configurable) before being send to the Kafka topic.
 *
 */
public final class UnixSocketSourceTask extends SourceTask {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(UnixSocketSourceTask.class);

    /** Name of the schema (consists of a single field MESSAGE_FIELD_NAME). */
    private static final String SCHEMA_NAME = "unix-socket";

    /** Name of the message field. */
    private static final String MESSAGE_FIELD_NAME = "message";

    private UnixSocketServerThread unixSocketServerThread;
    private Integer batchSize;
    private String[] topics;
    private Schema schema;

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
	List<SourceRecord> records = new ArrayList<>(0);
        while (!unixSocketServerThread.messages.isEmpty() &&
            records.size() < batchSize) {
            final String message = unixSocketServerThread.messages.poll();
            final Struct messageStruct = new Struct(schema);
            messageStruct.put(MESSAGE_FIELD_NAME, message);
            for (final String topic : topics) {
                SourceRecord record = new SourceRecord(
                    Collections.singletonMap("unix-socket", 0),
                    Collections.singletonMap("0", 0),
                    topic,
                    messageStruct.schema(),
                    messageStruct);
                records.add(record);
            }
        }
	return records;
    }

    @Override
    public void start(final Map<String, String> args) {
        // Get the configured batch size (that is the number of records that
        // should be batched together before sending to Kafka)
        try {
            batchSize =
                Integer.parseInt(args.get(UnixSocketSourceConfig.BATCH_SIZE));
        } catch (Exception e) {
            throw new ConnectException("Invalid configuration " +
                UnixSocketSourceConfig.BATCH_SIZE + " = " + batchSize);
        } 
        LOGGER.trace("{} = {}", UnixSocketSourceConfig.BATCH_SIZE, batchSize);

        // Get the configured Kafka topics
        topics = args.get(UnixSocketSourceConfig.KAFKA_TOPICS)
            .split(UnixSocketSourceConfig.KAFKA_TOPICS_DELIMITER);
        LOGGER.trace("{} = {}", UnixSocketSourceConfig.KAFKA_TOPICS, topics);

        // Get the 
        final String pathname = args.get(UnixSocketSourceConfig.PATHNAME);
        LOGGER.trace("{} = {}", UnixSocketSourceConfig.PATHNAME, pathname);

        schema = SchemaBuilder
            .struct()
            .name("unix-socket")
            .field(MESSAGE_FIELD_NAME, Schema.OPTIONAL_STRING_SCHEMA)
            .build();

        // Create and start the UNIX socket server with the configured pathname
	unixSocketServerThread = new UnixSocketServerThread(pathname);
        new Thread(unixSocketServerThread).start();
    }

    @Override
    public void stop() {
        unixSocketServerThread.stop();
    }

    @Override
    public String version() {
        return Version.getVersion();
    }
}
