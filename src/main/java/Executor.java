import com.google.common.base.Splitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Iterator;

/**
 * Created by minudika on 2/18/16.
 */
public class Executor {
    private BufferedReader br;
    private SiddhiManager siddhiManager = null;
    ///private Logger log = LoggerFactory.getLogger(Executor.class);
    private static Splitter splitter = Splitter.on(',');

    public static void main(String args[]) throws Exception {
        new TelnetClient().send();
    }

    public void execute(){
        String inStreamDefinition = "@config(async = 'true')define stream inputStream (ticker string, date string, time string, askPrice float);\n";
        String query = "from inputStream#window.length(10)\n" +
                "select ticker, avg(askPrice) as avg_price\n" +
                "insert into outputStream\n";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                //EventPrinter.print(events);

                for (org.wso2.siddhi.core.event.Event evt : events) {
                    Object[] dt = evt.getData();
                    // System.out.println(dt[0]);//log.info(dt[0]);
                   // log.error(dt[0]+" -> "+dt[1]);
                    System.err.println(dt[0]+" -> "+dt[1]);
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        /*try {
            br = new BufferedReader(new FileReader("/home/minudika/Projects/StreamingNoteBook/data/trades.csv"), 10 * 1024 * 1024);
            String line = br.readLine();
            while(line != null) {
                Iterator<String> dataStrIterator = splitter.split(line).iterator();
                String ticker = dataStrIterator.next();
                String date = dataStrIterator.next();
                String time = dataStrIterator.next();
                float askPrice;
                try {
                    askPrice = Float.parseFloat(dataStrIterator.next());
                }catch (Exception e){
                    askPrice = 1000f;
                }


                Object[] eventData = null;
                eventData = new Object[]{ticker,date,time,askPrice};
                inputHandler.send(eventData);
                Thread.sleep(500);
                line = br.readLine();
            }


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }

        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
*/
        executionPlanRuntime.shutdown();
    }
}
