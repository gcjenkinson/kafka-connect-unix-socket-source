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
import java.net.Socket;
import java.net.ServerSocket;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;

/***
 * UnixSocketServerThread handles clients connecting to the Unix domain socket.
 *
 * UnixSocketServerThread starts a new UnixSocketThread for each client that
 * connects to the Unix domain socket. This thread reads and buffers messages
 * from the socket.
 */
class UnixSocketServerThread implements Runnable {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(UnixSocketServerThread.class);

    private final ServerSocket serverSocket;
    final ConcurrentLinkedQueue<String> messages =
        new ConcurrentLinkedQueue<>();

    UnixSocketServerThread(final String pathname) {
        final File socketFile =
            new File(new File(System.getProperty("java.io.tmpdir")), pathname);

        try {
	    serverSocket = AFUNIXServerSocket.newInstance();
            serverSocket.bind(new AFUNIXSocketAddress(socketFile));
            LOGGER.trace("serverSocket {}", serverSocket);
        } catch (IOException e) {
            LOGGER.error("Error opening UNIX socket {}: {}",
		pathname, e.getMessage());
            throw new ConnectException("Error opening UNIX socket", e);
        }
    }

    @Override
    public void run() {
        while(true) {
            try {
                final Socket clientSocket = serverSocket.accept();
                // On accepting a client connection start a new
                // UnixSocketThread instance to read from the socket.
                new UnixSocketThread(clientSocket, messages).start();
            } catch (IOException e) {
                LOGGER.error("Error calling accept on serverSocket {}",
                    e.getMessage());
                throw new ConnectException(
                    "Error calling accept on serverSocket", e);
            }
        }
    }

   /***
     * Closes the Unix domain socket.
     */
    public void stop() {
        try {
            serverSocket.close();
        } catch (Exception e) {
            LOGGER.error("Error closing UNIX serverSocket {}", e.getMessage());
        }
    }
}
