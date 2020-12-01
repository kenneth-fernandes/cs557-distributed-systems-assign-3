
/**
 * KeyValueStoreHandler
 */

import java.util.HashMap;
import java.util.Map;

import org.apache.thrift.TException;

import keyvalstore.KeyValueStore;
import keyvalstore.ReplicaID;
import keyvalstore.Request;
import keyvalstore.SystemException;

public class KeyValueStoreHandler implements KeyValueStore.Iface {
    private String ipAddr;
    private int portNum;
    private Map<String, String> nodesInfo;
    private Map<Integer, String> keyValueData;

    public KeyValueStoreHandler(String ipAddr, int portNum, Map<String, String> nodesInfoIn) throws SystemException, TException {
        this.ipAddr = ipAddr;
        this.portNum = portNum;
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