/**
 * Created by minudika on 2/22/16.
 */
import com.google.common.base.Splitter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.ArrayList;
import java.util.List;

/**
 * Handler implementation for the object echo client.  It initiates the
 * ping-pong traffic between the object echo client and server by sending the
 * first message to the server.
 */
public class ObjectEchoClientHandler extends ChannelInboundHandlerAdapter {

    private SiddhiManager siddhiManager = new SiddhiManager();
    private Logger log = LoggerFactory.getLogger(Executor.class);
    private static Splitter splitter = Splitter.on(',');

    private static InputHandler inputHandler;
    private static ExecutionPlanRuntime executionPlanRuntime;
    String inStreamDefinition;
    String query;
    Runnable r;
    Thread t;


    private final List<Integer> firstMessage;

    /**
     * Creates a client-side handler.
     */
    public ObjectEchoClientHandler() {
        firstMessage = new ArrayList<Integer>(ObjectEchoClient.SIZE);
        for (int i = 0; i < ObjectEchoClient.SIZE; i ++) {
            firstMessage.add(Integer.valueOf(i));
        }

        inStreamDefinition = "@config(async = 'true')define stream inputStream (ticker string, date string, time string, askPrice double);\n";
        query = "from inputStream\n" +
                "select ticker, avg(askPrice) as avg_price\n" +
                "insert into outputStream\n";

        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);


        inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
/*
        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                //EventPrinter.print(events);

                for (org.wso2.siddhi.core.event.Event evt : events) {
                    Object[] dt = evt.getData();
                    // System.out.println(dt[0]);//log.info(dt[0]);
                    //    log.error(dt[0]+" -> "+dt[1]);
                    System.err.println(dt[0]+" -> "+dt[1]);
                }
            }
        });*/

       r = new Runnable() {
            public void run() {
                System.err.println("Thread started..");
                executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

                    @Override
                    public void receive(org.wso2.siddhi.core.event.Event[] events) {
                        EventPrinter.print(events);

                        for (org.wso2.siddhi.core.event.Event evt : events) {
                            Object[] dt = evt.getData();
                            // System.out.println(dt[0]);//log.info(dt[0]);
                            //    log.error(dt[0]+" -> "+dt[1]);
                            System.err.println(dt[0]+" -> "+dt[1]);
                        }
                    }
                });

            }
        };

        t = new Thread(r);
        // Lets run Thread in background..
        // Sometimes you need to run thread in background for your Timer application..
       t.start(); // starts thread in background..

        /*inStreamDefinition = "@config(async = 'true')define stream inputStream (ticker string, date string, time string, askPrice float);\n";
        query = "from inputStream#window.length(10)\n" +Object[] dt = evt.getData();
                "select ticker, avg(askPrice) as avg_price\n" +
                "insert into outputStream\n";

        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);


        inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();*/
        System.err.println("channel registered..");
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        // Send the first message if this handler is a client-side handler.
        //ctx.writeAndFlush(firstMessage);
       /* inStreamDefinition = "@config(async = 'true')define stream inputStream (ticker string, date string, time string, askPrice float);\n";
        query = "from inputStream#window.length(10)\n" +
                "select ticker, avg(askPrice) as avg_price\n" +
                "insert into outputStream\n";

        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);


        inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        System.err.println("channel registered..");

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                //EventPrinter.print(events);

                for (org.wso2.siddhi.core.event.Event evt : events) {
                    Object[] dt = evt.getData();
                    // System.out.println(dt[0]);//log.info(dt[0]);
                    //    log.error(dt[0]+" -> "+dt[1]);
                    System.err.println(dt[0]+" -> "+dt[1]);
                }
            }
        });*/
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws InterruptedException {
        // Echo back the received object to the server.
        ctx.write(msg);
        System.err.println("received : "+msg);

       /* inStreamDefinition = "@config(async = 'true')define stream inputStream (ticker string, date string, time string, askPrice float);\n";
        query = "from inputStream#window.length(10)\n" +
                "select ticker, avg(askPrice) as avg_price\n" +
                "insert into outputStream\n";

        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);


        inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();*/



        //Object eventData = ((Object [])msg)[0];
       // System.err.println(eventData.toString());
        inputHandler.send((Object [])msg);

    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        cause.printStackTrace();
        ctx.close();
    }
}