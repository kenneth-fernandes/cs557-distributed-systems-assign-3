import java.net.InetAddress;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;

import keyvalstore.*;

public class Server {

    public static KeyValueStoreHandler handler;
    public static KeyValueStore.Processor processor;
    public static int portNum;
    public static String ipAddr;
    
    public static void main(String[] args) {
        try {
    
          portNum = Integer.valueOf(args[0]);
          ipAddr = InetAddress.getLocalHost().getHostAddress();
          handler = new KeyValueStoreHandler(ipAddr, portNum);
          processor = new KeyValueStore.Processor(handler);
    
          Runnable simple = new Runnable() {
            public void run() {
              simple(processor);
            }
          };
          new Thread(simple).start();
        } catch (Exception x) {
          x.printStackTrace();
        }
      }
    
      public static void simple(KeyValueStore.Processor processor) {
        try {
          TServerTransport serverTransport = new TServerSocket(portNum);
          TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));
    
          System.out.println("Starting the simple server at " + ipAddr + ":" + portNum + " ...");
          server.serve();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

}
