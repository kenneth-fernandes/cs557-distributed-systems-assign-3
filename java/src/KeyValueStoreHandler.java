
/**
 * KeyValueStoreHandler
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
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
import keyvalstore.Value;

public class KeyValueStoreHandler implements KeyValueStore.Iface {
	private String ipAddr;
	private int portNum;
	private Map<Integer, Value> keyValueData;
	private List<NodeInfo> nodesInfo;
	private FileWriter oWriter;
	private File ofile;
	private Map<String, Value> hint;
	private BufferedReader reader;

	public KeyValueStoreHandler(String ipAddrIn, int portNumIn, List<NodeInfo> nodesInfoIn)
			throws SystemException, TException {
		ipAddr = ipAddrIn;
		portNum = portNumIn;
		nodesInfo = nodesInfoIn;
		hint = new HashMap<String, Value>();
		keyValueData= new HashMap<Integer, Value>();
		readLog();
		
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
				successfulPutCount = successfulPutCount + checkSuccessFailure(result, key, value, node);
				primaryNodeIndex = (primaryNodeIndex + 1) % 4;
			}
			
			if ((request.getLevel() == ConsistencyLevel.QUORUM && successfulPutCount < 2)
					|| (request.getLevel() == ConsistencyLevel.ONE && successfulPutCount < 1)) {
				throw new SystemException()
						.setMessage("Write for Consistency level " + request.getLevel() + " was not successful ");
			}	
			
			return true;

		} else {
			result = updateKeyStore(key, value, request.getTimestamp());
			return result;
		}

	}

	@Override
	public Value get(int key, Request request, ReplicaID replicaID) throws SystemException, TException {
		
		callForHints();		
		
		NodeInfo node;
		Value value = null;
		String latestValue= "";
		int dataRetrievalCount = 0;

		if (request.isCoordinator) {
			
			long timeStamp = 0;
			int primaryNodeIndex = key / 64;

			for (int i = 0; i < 3; i++) {

				node = nodesInfo.get(primaryNodeIndex);
				if (node.getIp().equals(ipAddr) && node.getPort() == portNum) {

					value = keyValueData.get(key);

					if (null != value && value.getTimestamp() > timeStamp) {
						timeStamp = value.getTimestamp();
						latestValue = value.getValue();
						dataRetrievalCount++;
					}

				} else {
					value = doRpcGet(node.getIp(), node.getPort(), key, request);

					if (null != value && value.getTimestamp() > timeStamp) {
						timeStamp = value.getTimestamp();
						latestValue = value.getValue();
						dataRetrievalCount++;
					} else {						
						value.setTimestamp(timeStamp);
						value.setValue(latestValue);
					}
				}
				primaryNodeIndex = (primaryNodeIndex + 1) % 4;
			}
			
			if ((request.getLevel() == ConsistencyLevel.QUORUM && dataRetrievalCount < 2)
					|| (request.getLevel() == ConsistencyLevel.ONE && dataRetrievalCount < 1)) {
				throw new SystemException()
						.setMessage("Read for Consistency level " + request.getLevel() + " was not successful ");
			}
			
			return value;
		} else {
			value = keyValueData.get(key);
			return value;
		}
		
	}

	@Override
	public Value getHints(String ip, int port) throws SystemException, TException {

		return hint.get(ip + ":" + port);
	}

	public void writeAheadLog(int key, String value, Long timestamp) {

		try {
			
			String path = System.getProperty("user.dir") + File.separator;
			String logFilename = ipAddr + "_" + portNum + "_log.txt";

			oWriter = new FileWriter(path + logFilename, true);
			oWriter.write(timestamp + "|" + key + "|" + value + System.lineSeparator());

			oWriter.flush();
			oWriter.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void readLog() {

		try {

			String path = System.getProperty("user.dir") + File.separator;
			String logFilename = ipAddr + "_" + portNum + "_log.txt";

			if (Files.exists(Paths.get(path + logFilename))) {
				reader = new BufferedReader(new FileReader(new File(path + logFilename)));

				String line = "";
				Value logValue;

				while ((line = reader.readLine()) != null) {

					logValue = new Value();
					String[] temp = line.split("|");

					logValue.setTimestamp(Long.parseLong(temp[0]));
					logValue.setTimestampIsSet(true);
					logValue.setValue(temp[2]);
					logValue.setValueIsSet(true);

					keyValueData.put(Integer.parseInt(temp[1]), logValue);

				}
				
				reader.close();

			} else {
				ofile = new File(path + logFilename);
				ofile.createNewFile();
			}
			

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

	public Value doRpcGet(String ip, int port, int key, Request req) throws SystemException, TException {

		ThriftConnection conn = getConn(port, ip);
		KeyValueStore.Client client = conn.getClient();

		Request request = new Request();

		request.setIsCoordinator(false);
		request.setIsCoordinatorIsSet(true);

		request.setLevel(req.getLevel());
		request.setLevelIsSet(true);

		ReplicaID replicaID = null;

		Value result = client.get(key, request, replicaID);
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

	public int checkSuccessFailure(boolean flag, int key, String value, NodeInfo node) {

		if (flag)
			return 1;
		else {
			Value val = new Value();
			val.setKey(key);
			val.setValue(value);
			hint.put(node.getIp() + ":" + node.getPort(), val);
			return 0;
		}
	}

	public void callForHints() throws SystemException, TException {
		Value val;
		for (NodeInfo node : nodesInfo) {
			if (!(node.getIp().equals(ipAddr) && node.getPort() == portNum)) {
				
				ThriftConnection conn = getConn(node.getPort(), node.getIp());
				
				if (null != conn.getTransport()) {
					KeyValueStore.Client client = conn.getClient();
					val = client.getHints(ipAddr, portNum);
					if (null != val)
						updateKeyStore(val.getKey(), val.getValue(), val.getTimestamp());
					conn.getTransport().close();
				}
			}
		}
	}

	public ThriftConnection getConn(int port, String ip) {
		
		ThriftConnection conn = new ThriftConnection();
		try {

			TTransport transport;
			transport = new TSocket(ip, port);
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);

			conn.setTransport(transport);
			conn.setClient(new KeyValueStore.Client(protocol));
			
		}catch(TException e) {
			return conn;
		}
		return conn;
		
	}

}