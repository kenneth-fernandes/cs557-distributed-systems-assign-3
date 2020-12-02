namespace java keyvalstore

exception SystemException {
  1: optional string message
}

enum ConsistencyLevel {
	ONE;
	QUORUM;
}

struct Request {
  1: required ConsistencyLevel level;
	2: required bool isCoordinator;
  3: optional i64 timestamp;
}

struct Value {
  1: i64 timestamp;
	2: string value;
	3: i32 key;
}

struct ReplicaID {
  1: string id;
  2: string ip;
  3: i32 port;
}

service KeyValueStore {
  bool put(1: i32 key, 2: string value, 3: Request request, 4: ReplicaID replicaID)
    throws (1: SystemException systemException),

  string get(1: i32 key, 2: Request request, 3: ReplicaID replicaID)
    throws (1: SystemException systemException),

  Value getHints()
    throws (1: SystemException systemException),
}