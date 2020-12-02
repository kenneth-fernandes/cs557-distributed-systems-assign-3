
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
            request.setTimestamp(System.currentTimeMillis());
            request.setTimestampIsSet(true);
            int primaryNodeIndex = key / 64;
            for (int i = 0; i < 3; i++) {
                node = nodesInfo.get(primaryNodeIndex);
                if (node.getIp().equals(ipAddr) && node.getPort() == portNum)
                    updateKeyStore(key, value, request.getTimestamp());
                else
                    doRpcPut(node.getIp(), node.getPort(), key, value, request);
                primaryNodeIndex = (primaryNodeIndex + 1) % 4;
            }

        } else {
            updateKeyStore(key, value, request.getTimestamp());
        }
        return result;
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
                    result = doRpcGet(node.getIp(), node.getPort(), key, value, request);
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

    public boolean doRpcPut(String ip, int port, int key, String value, Request req)
            throws SystemException, TException {

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

        request.setTimestamp(req.getTimestamp());
        request.setTimestampIsSet(true);

        ReplicaID replicaID = null;

        boolean result = client.put(key, value, request, replicaID);
        transport.close();
        return result;
    }

    public String doRpcGet(String ip, int port, int key, Request req) throws SystemException, TException {

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

        String result = client.get(key, request, replicaID);
        transport.close();
        return result;
    }

    public void updateKeyStore(int key, String valueIn, Long timestamp) {
        writeAheadLog(key, valueIn, timestamp);
        Value value = new Value();
        value.setTimestamp(timestamp);
        value.setValue(valueIn);
        if (keyValueData.containsKey(key))
            keyValueData.replace(key, value);
        else
            keyValueData.put(key, value);
    }

}