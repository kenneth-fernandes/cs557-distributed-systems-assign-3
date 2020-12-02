
/**
 * KeyValueStoreHandler
 */

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
	private Map<Integer, Value> keyValueData;
	private List<NodeInfo> nodesInfo;
	private FileWriter oWriter;
	private File ofile;
	private Value hint;

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
		int successfulPutCount = 0;
		boolean result = false;

		if (request.isCoordinator) {
			request.setTimestamp(System.currentTimeMillis());
			request.setTimestampIsSet(true);
			int primaryNodeIndex = key / 64;

			for (int i = 0; i < 3; i++) {
				node = nodesInfo.get(primaryNodeIndex);
				if (node.getIp().equals(ipAddr) && node.getPort() == portNum)
					result = updateKeyStore(key, value, request.getTimestamp());
				else
					result = doRpcPut(node.getIp(), node.getPort(), key, value, request);
				successfulPutCount = checkSuccessFailure(result, key, value);
				primaryNodeIndex = (primaryNodeIndex + 1) % 4;
			}

		} else {
			updateKeyStore(key, value, request.getTimestamp());
		}

		SystemException e = new SystemException();

		if ((request.getLevel() == ConsistencyLevel.QUORUM && successfulPutCount < 2)
				|| (request.getLevel() == ConsistencyLevel.ONE && successfulPutCount < 1)) {
			e.setMessage("Write for Consistency level " + request.getLevel() + " is not successful ");
			throw e;
		}

		return true;
	}

	@Override
	public String get(int key, Request request, ReplicaID replicaID) throws SystemException, TException {
		NodeInfo node;
		String result = "";
		Value value;

		if (request.isCoordinator) {

			int dataRetrievalCount = 0;
			long timeStamp = 0;
			String prevResult = "";

			int primaryNodeIndex = key / 64;
			int consistencyLevel = request.getLevel() == ConsistencyLevel.ONE ? 1 : 2;

			for (int i = 0; i < consistencyLevel; i++) {
				node = nodesInfo.get(primaryNodeIndex);
				if (node.getIp().equals(ipAddr) && node.getPort() == portNum) {
					value = keyValueData.containsKey(key) ? keyValueData.get(key) : null;
					result = value == null ? value.getValue() + " " + value.getTimestamp() : "";
				} else {
					result = doRpcGet(node.getIp(), node.getPort(), key, request);
				}
				primaryNodeIndex = (primaryNodeIndex + 1) % 4;

				String[] split = result.split(" ");
				result = Long.parseLong(split[1]) > timeStamp ? result : prevResult;
				dataRetrievalCount = !result.equals("") ? dataRetrievalCount + 1 : dataRetrievalCount;
				prevResult = result;
			}
		} else {
			value = keyValueData.containsKey(key) ? keyValueData.get(key) : "";
			result = value == null ? value.getValue() + " " + value.getTimestamp() : "";
		}
		return result;
	}

	public void writeAheadLog(int key, String value, Long timestamp) {

		try {
			String path = System.getProperty("user.dir") + File.separator;
			String logFilename = ipAddr + "_" + portNum + "_log.txt";
			if (!Files.exists(Paths.get(path + logFilename))) {
				ofile = new File(path + logFilename);
				ofile.createNewFile();
				oWriter = new FileWriter(ofile, true);

				oWriter.write(timestamp + " " + key + " " + value);

			} else {
				oWriter = new FileWriter(ofile, true);
				oWriter.write(timestamp + " " + key + " " + value);
			}

			oWriter.flush();
			oWriter.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public boolean doRpcPut(String ip, int port, int key, String value, Request req) {

		try {
			
			ThriftConnection conn = getConn(port, ip);			
			KeyValueStore.Client client = conn.getClient();

			Request request = new Request();

			request.setIsCoordinator(false);
			request.setIsCoordinatorIsSet(true);

			request.setLevel(req.getLevel());
			request.setLevelIsSet(true);

			request.setTimestamp(req.getTimestamp());
			request.setTimestampIsSet(true);

			ReplicaID replicaID = null;

			boolean result = client.put(key, value, request, replicaID);
			conn.getTransport().close();
			
			return result;
			
		} catch (TException e) {
			return false;
		}
	}

	public String doRpcGet(String ip, int port, int key, Request req) throws SystemException, TException {

		ThriftConnection conn = getConn(port, ip);			
		KeyValueStore.Client client = conn.getClient();

		Request request = new Request();

		request.setIsCoordinator(false);
		request.setIsCoordinatorIsSet(true);

		request.setLevel(req.getLevel());
		request.setLevelIsSet(true);

		ReplicaID replicaID = null;

		String result = client.get(key, request, replicaID);
		conn.getTransport().close();
		return result;
	}

	public Boolean updateKeyStore(int key, String valueIn, Long timestamp) {
		writeAheadLog(key, valueIn, timestamp);
		Value value = new Value();
		value.setTimestamp(timestamp);
		value.setValue(valueIn);
		if (keyValueData.containsKey(key))
			keyValueData.replace(key, value);
		else
			keyValueData.put(key, value);
		return true;
	}
	
	public int checkSuccessFailure(boolean flag, int key, String value) {
		
		if (flag)
			return 1;
		else {
			Value val = new Value();
			val.setKey(key);
			val.setValue(value);
			hint = val;
			return 0;
		}
	}
	
	
	public void callForHints() throws TTransportException {
		for(NodeInfo node : nodesInfo) {
			if(!(node.getIp().equals(ipAddr) && node.getPort() == portNum)) {
				ThriftConnection conn = getConn(node.getPort(), node.getIp());
				KeyValueStore.Client client = conn.getClient();
			}
		}
	}
	
	
	public ThriftConnection getConn(int port, String ip) throws TTransportException{
		
		ThriftConnection conn = new ThriftConnection();
		
		TTransport transport;
		transport = new TSocket(ip, port);
		transport.open();
		TProtocol protocol = new TBinaryProtocol(transport);
		
		conn.setTransport(transport);
		conn.setClient(new KeyValueStore.Client(protocol));

		
		return conn;
	}
	

}