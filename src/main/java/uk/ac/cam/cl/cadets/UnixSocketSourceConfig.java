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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

/***
 * Specifies configuration options for the UnixSocketSourceConnector.
 */
class UnixSocketSourceConfig extends AbstractConfig {

    static final ConfigDef CONFIG_DEF = new ConfigDef();

    /** Kafka topics to send messages to */
    static final String KAFKA_TOPICS = "topics";
    static final String KAFKA_TOPICS_DELIMITER = ",";

    /** Number of messages to batch before sending to Kafka */
    static final String BATCH_SIZE = "batch.size";
    static final String BATCH_SIZE_DOC =
        "Number of records to batch before sending to Kafka";
    static final int BATCH_SIZE_DEFAULT = 100;

    /* Pathname of the Unix domain socket */
    static final String PATHNAME = "pathname";
    static final String PATHNAME_DOC = "Unix socket pathname";
    static final String PATHNAME_DEFAULT = "";

    static {
        CONFIG_DEF.define(BATCH_SIZE, ConfigDef.Type.INT,
            BATCH_SIZE_DEFAULT, ConfigDef.Importance.HIGH, BATCH_SIZE_DOC);
        CONFIG_DEF.define(PATHNAME, ConfigDef.Type.STRING,
            PATHNAME_DEFAULT, ConfigDef.Importance.HIGH, PATHNAME_DOC);
    }

    UnixSocketSourceConfig(Map<? ,?> props) {
        super(CONFIG_DEF, props);
    }
}
