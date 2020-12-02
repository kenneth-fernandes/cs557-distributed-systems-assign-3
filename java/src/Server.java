import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import keyvalstore.KeyValueStore;

public class Server {

	public static KeyValueStoreHandler handler;
	public static KeyValueStore.Processor processor;
	public static int portNum;
	public static String ipAddr;
	private static BufferedReader reader;

	public static void main(String[] args) {
		try {

			String workingDirectory = System.getProperty("user.dir") + File.separator + "config.txt";
			String line = "";
			List<NodeInfo> nodesInfo = new ArrayList<NodeInfo>();
			reader = new BufferedReader(new FileReader(new File(workingDirectory)));
			System.out.println(workingDirectory);

			while ((line = reader.readLine()) != null) {

				NodeInfo node = new NodeInfo();
				String[] input = line.split(" ");

				node.setIp(input[0]);
				node.setPort(Integer.parseInt(input[1]));
				node.setLowRange(input[2]);
				node.setUpperRange(input[3]);
				nodesInfo.add(node);

			}
			reader.close();

			portNum = Integer.valueOf(args[0]);
			ipAddr = InetAddress.getLocalHost().getHostAddress();
			handler = new KeyValueStoreHandler(ipAddr, portNum, nodesInfo);
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
