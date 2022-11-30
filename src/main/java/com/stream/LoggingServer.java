package com.stream;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Simulate a streaming source from a web server
 * A basic example. Nowhere near production standard.
 * Run as a regular Java application. Expect nothing in console log.
 *
 * If client disconnects, must restart the server!
 * */
public class LoggingServer {
	public static void main(String[] args) throws IOException, InterruptedException {
	    ServerSocket echoSocket = new ServerSocket(8989);
	    Socket socket = echoSocket.accept();

	    while (true) {
	    	PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
	    	double sample = Math.random();
	    	String level = "DEBUG";
	    	if (sample < 0.0001) {
	    		level ="FATAL";
	    	} else if (sample < 0.01) {
	    		level = "ERROR";
	    	} else if (sample < 0.1) {
	    		level = "WARN";
	    	} else if (sample < 0.5) {
	    		level = "INFO";
	    	}
	    	out.println(level + "," + new java.util.Date());
	    	Thread.sleep(1);
	    }
	}
}
