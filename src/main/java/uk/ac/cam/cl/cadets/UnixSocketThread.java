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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/***
 * UnixSocketThread is a thread that reads and buffers input from a Unix socket.
 *
 * A UnixSocketThread is started for each client of the Unix domain socket.
 * The thread reads line byt line from the Unix domain socket using a
 * BufferredReader. Indiviudal messages are buffered in the messages data
 * structure (a COncurrentLinkedQueue). Messages are read from the queue by
 * a UnixSocketSourceTask instance and then inputted into the Kafka topic.
 */
class UnixSocketThread extends Thread {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(UnixSocketThread.class);
    private final Socket clientSocket;
    private final ConcurrentLinkedQueue<String> messages;
    
    UnixSocketThread(final Socket clientSocket,
	final ConcurrentLinkedQueue<String> messages) {
        this.clientSocket = clientSocket;
        this.messages = messages;
    }

    @Override
    public void run() {
	InputStream input;
	BufferedReader br;
	try {
            input = clientSocket.getInputStream();
            br = new BufferedReader(new InputStreamReader(input));

            String line = null;
            while (true) {
                try {
                    line = br.readLine();
                    if (line == null) {
                        clientSocket.close();
                    } else {
                        messages.add(line);
                    }
                } catch (IOException e) {
                    LOGGER.error("Error reading from UnixSocket {}",
			e.getMessage());
                    throw new ConnectException(e);
                }
            }
        } catch (IOException e) {
            LOGGER.error("Error constructing UnixSocketThread {}",
                e.getMessage());
            throw new ConnectException(e);
        }
    }
}

