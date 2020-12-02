
/**
 * KeyValueStoreHandler
 */

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import keyvalstore.ConsistencyLevel;
import keyvalstore.KeyValueStore;
import keyvalstore.ReplicaID;
import keyvalstore.Request;
import keyvalstore.SystemException;

public class KeyValueStoreHandler implements KeyValueStore.Iface {
	private String ipAddr;
	private int portNum;
	private Map<Integer, String> keyValueData;
	private List<NodeInfo> nodesInfo;
	private FileWriter oWriter;
	private File ofile;

	public KeyValueStoreHandler(String ipAddrIn, int portNumIn, List<NodeInfo> nodesInfoIn)
			throws SystemException, TException {
		ipAddr = ipAddrIn;
		portNum = portNumIn;
		nodesInfo = nodesInfoIn;
		System.out.println("KeyValueStoreHandler constructor");
	}

	@Override
	public boolean put(int key, String value, Request request, ReplicaID replicaID) throws SystemException, TException {
		NodeInfo node;
		boolean result = false;

		if (request.isCoordinator) {
			int primaryNodeIndex = key / 64;
			for (int i = 0; i < 3; i++) {
				node = nodesInfo.get(primaryNodeIndex);
				if (node.getIp().equals(ipAddr) && node.getPort() == portNum)
					updateKeyStore(key, value);
				else
					doRpc(node.getIp(), node.getPort(), key, value, request);
				primaryNodeIndex = (primaryNodeIndex + 1) % 4;
			}

		} else {
			updateKeyStore(key, value);
		}
		return result;
	}

	@Override
	public String get(int key, Request request, ReplicaID replicaID) throws SystemException, TException {
		return "";
	}

	public void writeAheadLog(int key, String value) {

		try {
			String path = System.getProperty("user.dir") + File.separator;
			String logFilename = ipAddr + "_" + portNum + "_log.txt";
			if (!Files.exists(Paths.get(path + logFilename))) {
				ofile = new File(path + logFilename);
				ofile.createNewFile();
				oWriter = new FileWriter(ofile, true);

				oWriter.write(System.currentTimeMillis() + " " + key + " " + value);

			} else {
				oWriter = new FileWriter(ofile, true);
				oWriter.write(System.currentTimeMillis() + " " + key + " " + value);
			}

			oWriter.flush();
			oWriter.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public boolean doRpc(String ip, int port, int key, String value, Request req) throws SystemException, TException {

		TTransport transport;
		transport = new TSocket(ip, port);
		transport.open();

		TProtocol protocol = new TBinaryProtocol(transport);
		KeyValueStore.Client client = new KeyValueStore.Client(protocol);

		Request request = new Request();

		request.setIsCoordinator(false);
		request.setIsCoordinatorIsSet(true);

		request.setLevel(req.getLevel());
		request.setLevelIsSet(true);

		ReplicaID replicaID = null;

		boolean result = client.put(key, value, request, replicaID);
		transport.close();
		return result;
	}

	public void updateKeyStore(int key, String value) {

		writeAheadLog(key, value);

		if (keyValueData.containsKey(key))
			keyValueData.replace(key, value);
		else
			keyValueData.put(key, value);
	}

}