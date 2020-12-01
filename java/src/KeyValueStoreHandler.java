
/**
 * KeyValueStoreHandler
 */

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import keyvalstore.*;

public class KeyValueStoreHandler implements KeyValueStore.Iface {
    private String ipAddr;
    private int portNum;

    KeyValueStoreHandler(String ipAddr, int portNum) throws SystemException, TException {
        this.ipAddr = ipAddr;
        this.portNum = portNum;
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