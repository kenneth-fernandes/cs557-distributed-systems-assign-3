
/**
 * KeyValueStoreHandler
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;

import keyvalstore.KeyValueStore;
import keyvalstore.ReplicaID;
import keyvalstore.Request;
import keyvalstore.SystemException;

public class KeyValueStoreHandler implements KeyValueStore.Iface {
    private String ipAddr;
    private int portNum;
    private Map<Integer, String> keyValueData;
    private List<NodeInfo> nodesInfo;

    public KeyValueStoreHandler(String ipAddrIn, int portNumIn, List<NodeInfo> nodesInfoIn)
            throws SystemException, TException {
        ipAddr = ipAddrIn;
        portNum = portNumIn;
        nodesInfo = nodesInfoIn;
        System.out.println("KeyValueStoreHandler constructor");
    }

    @Override
    public boolean put(int key, String value, Request request, ReplicaID replicaID) throws SystemException, TException {
        return true;
    }

    @Override
    public String get(int key, Request request, ReplicaID replicaID) throws SystemException, TException {
        return "";
    }
}