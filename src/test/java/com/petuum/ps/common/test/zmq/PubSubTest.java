package com.petuum.ps.common.test.zmq;

import org.zeromq.ZContext;
import org.zeromq.ZMQ;

import java.nio.ByteBuffer;

/**
 * Created by suyuxin on 14-9-4.
 */
public class PubSubTest {
    public static ZContext zmqContext = new ZContext(1);
    public static Thread publisher = new Thread(new Runnable() {
        public void run() {
            ZMQ.Socket pub = zmqContext.createSocket(ZMQ.PUB);
            pub.bind("inproc://pub");
            String test = "Hell World";
            for(int i = 0; i < 10; i++) {
                pub.send(test, 0);
                System.out.println("Send ... " + String.valueOf(i));
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    });

    public static Thread subscriber = new Thread(new Runnable() {
        public void run() {
            ZMQ.Socket sub = zmqContext.createSocket(ZMQ.SUB);
            sub.connect("inproc://pub");
            sub.subscribe("".getBytes());
            while(!Thread.currentThread().isInterrupted()) {
                System.out.println(sub.recvStr());
            }
        }
    });

    public static void main(String[] args) throws InterruptedException {
        publisher.start();
        subscriber.start();
        publisher.join();
        subscriber.join();
    }
}
