import com.google.common.base.Splitter;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.io.BufferedReader;
import java.util.ArrayList;

/**
 * Handles a client-side channel.
 */
@Sharable
public class TelnetClientHandler implements ChannelInboundHandler {private BufferedReader br;
    private SiddhiManager siddhiManager = new SiddhiManager();
   private Logger log = LoggerFactory.getLogger(Executor.class);
    private static Splitter splitter = Splitter.on(',');

    private InputHandler inputHandler;
    private ExecutionPlanRuntime executionPlanRuntime;
    String inStreamDefinition;
    String query;

    public void channelRegistered(ChannelHandlerContext channelHandlerContext) throws Exception {

        inStreamDefinition = "@config(async = 'true')define stream inputStream (ticker string, date string, time string, askPrice float);\n";
        query = "from inputStream#window.length(10)\n" +
                "select ticker, avg(askPrice) as avg_price\n" +
                "insert into outputStream\n";

        executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);


        inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        System.err.println("channel registered..");
    }

    public void channelUnregistered(ChannelHandlerContext channelHandlerContext) throws Exception {

    }

    public void channelActive(ChannelHandlerContext channelHandlerContext) throws Exception {

    }

    public void channelInactive(ChannelHandlerContext channelHandlerContext) throws Exception {

    }

    public void channelRead(ChannelHandlerContext channelHandlerContext, Object object) throws Exception {

        System.err.println("Response received : "+object.toString());
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
        });

        Object eventData = null;
        ArrayList<Object> list = (ArrayList<Object>)object;
        for(Object o:list){
            eventData = o;
            inputHandler.send((Event) eventData);
            Thread.sleep(500);
        }
    }

    public void channelReadComplete(ChannelHandlerContext channelHandlerContext) throws Exception {

    }

    public void userEventTriggered(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {

    }

    public void channelWritabilityChanged(ChannelHandlerContext channelHandlerContext) throws Exception {

    }

    public void handlerAdded(ChannelHandlerContext channelHandlerContext) throws Exception {

    }

    public void handlerRemoved(ChannelHandlerContext channelHandlerContext) throws Exception {

    }

    public void exceptionCaught(ChannelHandlerContext channelHandlerContext, Throwable throwable) throws Exception {

    }
}
