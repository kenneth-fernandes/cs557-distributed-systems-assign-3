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
			if (args[2].equals(ConsistencyLevel.ONE.toString()))
				perform(client, ConsistencyLevel.ONE);
			else
				perform(client, ConsistencyLevel.QUORUM);
		} catch (TException e) {
			e.printStackTrace();
		}

	}

	private static void perform(KeyValueStore.Client client, ConsistencyLevel level) throws TException {

		Request request = new Request();
		request.setLevel(level);
		request.setIsCoordinator(true);
		request.setIsCoordinatorIsSet(true);

		ReplicaID replicaID = null;
		boolean flag = client.put(64, "First key value for 23", request, replicaID);
		System.out.println("Put returned value: " + flag);
		flag = client.put(23, "I am the changed value", request, replicaID);
		System.out.println("Put returned value again: " + flag);
		Value val = client.get(64, request, replicaID);

		System.out.println("Value returned: " + val.getValue());

	}
}
