import keyvalstore.*;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class Client {

	public static void main(String[] args) {
		try {

			TTransport transport;
			transport = new TSocket(args[0], Integer.valueOf(args[1]));
			transport.open();

			TProtocol protocol = new TBinaryProtocol(transport);
			KeyValueStore.Client client = new KeyValueStore.Client(protocol);

			perform(client);
		} catch (TException e) {
			e.printStackTrace();
		}

	}

	private static void perform(KeyValueStore.Client client) throws TException {
		
		Request request = new Request();
		request.setLevel(ConsistencyLevel.QUORUM);
		request.setIsCoordinator(true);
		request.setIsCoordinatorIsSet(true);
		
		ReplicaID replicaID = null;
		boolean flag = client.put(23, "First key value for 23", request, replicaID);
		System.out.println("Put returned value: "+flag);
		
		Value val = client.get(23, request, replicaID);
		
		System.out.println("Value returned: "+val.getValue());

	}
}
