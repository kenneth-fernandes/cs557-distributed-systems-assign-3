import org.apache.thrift.transport.TTransport;

import keyvalstore.KeyValueStore;

public class ThriftConnection {
	
	KeyValueStore.Client client;
	TTransport transport;
	
	public KeyValueStore.Client getClient() {
		return client;
	}
	public void setClient(KeyValueStore.Client client) {
		this.client = client;
	}
	public TTransport getTransport() {
		return transport;
	}
	public void setTransport(TTransport transport) {
		this.transport = transport;
	}

}