package com.petuum.ps.common.test.zmq;

import org.zeromq.ZContext;
import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMsg;

/**
 * Created by suyuxin on 14-9-5.
 */
public class ZMQFreelanceTest {

    public static ZContext ctx = new ZContext(1);

    public static Thread client = new Thread(new Runnable() {
        public void run() {
            //  Create new freelance client object
            //flcliapi client = new flcliapi();
            ZMQ.Socket router = ctx.createSocket(ZMQ.ROUTER);

            router.bind("ipc://test1");
            router.connect("ipc://test2");



            //  Send a bunch of name resolution 'requests', measure time
            int requests = 100;
            long start = System.currentTimeMillis();
            while (requests-- > 0) {

                //boolean reply = request.send(router);
                router.send("1".getBytes());
                boolean reply = router.send("random name");
                //ZMsg reply = client.request(request);
                if (reply == false) {
                    System.out.printf("E: name service not available, aborting\n");
                    break;
                }
                System.out.println("send request " + String.valueOf(requests));
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.printf("Average round trip cost: %d usec\n",
                    (int) (System.currentTimeMillis() - start) / 10);
        }
    });

    public static Thread server = new Thread(new Runnable() {
        public void run() {
            boolean verbose = false;


            //  Prepare server socket with predictable identity
            String bindEndpoint = "ipc://test2";
            String connectEndpoint = "1";
            ZMQ.Socket server = ctx.createSocket(ZMQ.ROUTER);
            server.setIdentity(connectEndpoint.getBytes());
            server.bind(bindEndpoint);
            System.out.printf ("I: service is ready at %s\n", bindEndpoint);

            while (!Thread.currentThread().isInterrupted()) {
                ZMsg request = ZMsg.recvMsg(server);
                if (verbose && request != null)
                    request.dump(System.out);

                if (request == null)
                    break;          //  Interrupted
                System.out.println(request.toString());
                //  Frame 0: identity of client
                //  Frame 1: PING, or client control frame
                //  Frame 2: request body
                ZFrame identity = request.pop();
                ZFrame control = request.pop();
                ZMsg reply = new ZMsg();
                if (control.equals("PING"))
                    reply.add("PONG");
                else {
                    reply.add(control);
                    reply.add("OK");
                }
                request.destroy();
                reply.push(identity);
                if (verbose && reply != null)
                    reply.dump(System.out);
                reply.send(server);
            }
            if (Thread.currentThread().isInterrupted())
                System.out.printf ("W: interrupted\n");
        }
    });

    public static void main(String[] args) throws InterruptedException {
        server.start();
        client.start();
        server.join();
        client.join();
    }
}
