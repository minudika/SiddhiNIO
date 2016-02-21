import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;


import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Simplistic telnet client.
 */
public final class TelnetClient {

    static final boolean SSL = System.getProperty("ssl") != null;
    static final String HOST = System.getProperty("host", "127.0.0.1");
    static final int PORT = Integer.parseInt(System.getProperty("port", SSL? "8992" : "8023"));

    public  void send() throws Exception {
        // Configure SSL.
        final SslContext sslCtx;
        if (SSL) {
            sslCtx = SslContextBuilder.forClient()
                    .trustManager(InsecureTrustManagerFactory.INSTANCE).build();
        } else {
            sslCtx = null;
        }

        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .handler(new TelnetClientInitializer(sslCtx));

            // Start the connection attempt.
            Channel ch = b.connect(HOST, PORT).sync().channel();

            // Read commands from the stdin.
            ChannelFuture lastWriteFuture = null;
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
                       for (;;) {
                               String line = "";//in.readLine();
                           /*  if (line == null) {
                                  break;
                                  }*/
                             // Sends the received line to the server.
                              lastWriteFuture = ch.writeAndFlush("req"+ "\r\n");
                           Thread.sleep(1000);

                               // If user typed the 'bye' command, wait until the server closes
                                // the connection.

                           }


            // Sends the received line to the server.
            /*while(true) {
                lastWriteFuture = ch.writeAndFlush("req" + "\r\n");
                System.err.println("Requested..");
                Thread.sleep(500);
            }*/
            //lastWriteFuture = ch.writeAndFlush("req" + "\r\n");
            /*System.err.println("Requested..");
            Thread.sleep(500);
            lastWriteFuture = ch.writeAndFlush("req" + "\r\n");
            System.err.println("Requested..");
            Thread.sleep(500);
            lastWriteFuture = ch.writeAndFlush("req" + "\r\n");
            System.err.println("Requested..");
            Thread.sleep(500);
            lastWriteFuture = ch.writeAndFlush("req" + "\r\n");
            System.err.println("Requested..");
            Thread.sleep(500);
*/
            // If user typed the 'bye' command, wait until the server closes
            // the connection.

          //  ch.closeFuture().sync();


            // Wait until all messages are flushed before closing the channel.
           /* if (lastWriteFuture != null) {
                lastWriteFuture.sync();
            }*/
        } finally {
            group.shutdownGracefully();
        }
    }
}
