pub mod messages {
    use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId};
    use serde::{Deserialize, Serialize};
    use crate::common::ds::TransactionCommand;
    use super::{
        ds::{Command, CommandId, DataSourceCommand},
        utils::Timestamp,
    };

    pub type RequestIdentifier = String;

    pub type TableName = String;

    #[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
    pub enum RSMIdentifier {
        Transaction,
        Shard(TableName)
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum RegistrationMessage {
        NodeRegister(NodeId),
        ClientRegister,
        CliClientRegister(RequestIdentifier),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClusterMessage {
        OmniPaxosMessage(RSMIdentifier, OmniPaxosMessage<Command>),
        LeaderStartSignal(Timestamp),
        ReadRequest(RequestIdentifier, ConsistencyLevel, DataSourceCommand),
        ReadResponse(RequestIdentifier, ConsistencyLevel, usize, Option<String>)
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ConsistencyLevel {
        Local,
        Leader,
        Linearizable
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClientMessage {
        Append(CommandId, TransactionCommand),
        Read(RequestIdentifier, ConsistencyLevel, DataSourceCommand),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ServerMessage {
        Write(CommandId),
        Read(CommandId, Option<String>),
        StartSignal(Timestamp),
        ReadResponse(RequestIdentifier, ConsistencyLevel, Option<String>)
    }

    impl ServerMessage {
        pub fn command_id(&self) -> CommandId {
            match self {
                ServerMessage::Write(id) => *id,
                ServerMessage::Read(id, _) => *id,
                ServerMessage::StartSignal(_) => unimplemented!(),
                ServerMessage::ReadResponse(_, _, _) => unimplemented!()
            }
        }
    }
}

pub mod ds {
    use std::fmt::Debug;
    use omnipaxos::{macros::Entry};
    use serde::{Deserialize, Serialize};
    use crate::common::messages::TableName;

    pub type CommandId = usize;
    pub type ClientId = u64;
    pub type NodeId = omnipaxos::util::NodeId;
    pub type InstanceId = NodeId;

    #[derive(Debug, Clone, Entry, Serialize, Deserialize)]
    pub struct Command {
        pub client_id: ClientId,
        pub coordinator_id: NodeId,
        pub id: CommandId,
        pub cmd_type: CommandType,
        pub ds_cmd: Option<DataSourceCommand>,
        pub tx_cmd: Option<TransactionCommand>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct RowData {
        pub row_name: String,
        pub row_value: String
    }
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DataSourceObject {
        pub table_name: TableName,
        pub row_data: Vec<RowData>
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum DataSourceQueryType {
        INSERT,
        UPDATE,
        READ
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct DataSourceCommand {
        pub data_source_object: Option<DataSourceObject>,
        pub query_type: DataSourceQueryType,
        pub query_params: Option<QueryParams>,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TransactionCommand {
        pub tx_id: String,
        pub data_source_commands: Vec<DataSourceCommand>
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum CommandType {
        DatasourceCommand,
        TransactionCommand
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct QueryParams {
        pub table_name: TableName,
        pub select_all: bool,
        pub select_columns: Option<Vec<String>>,
        //SELECT * from table_name
    }

    /*#[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct KVSnapshot {
        snapshotted: HashMap<String, String>,
        deleted_keys: Vec<String>,
    }

    impl Snapshot<Command> for KVSnapshot {
        fn create(entries: &[Command]) -> Self {
            let mut snapshotted = HashMap::new();
            let mut deleted_keys: Vec<String> = Vec::new();
            for e in entries {
                match &e.kv_cmd {
                    KVCommand::Put(key, value) => {
                        snapshotted.insert(key.clone(), value.clone());
                    }
                    KVCommand::Delete(key) => {
                        if snapshotted.remove(key).is_none() {
                            // key was not in the snapshot
                            deleted_keys.push(key.clone());
                        }
                    }
                    KVCommand::Get(_) => (),
                }
            }
            // remove keys that were put back
            deleted_keys.retain(|k| !snapshotted.contains_key(k));
            Self {
                snapshotted,
                deleted_keys,
            }
        }

        fn merge(&mut self, delta: Self) {
            for (k, v) in delta.snapshotted {
                self.snapshotted.insert(k, v);
            }
            for k in delta.deleted_keys {
                self.snapshotted.remove(&k);
            }
            self.deleted_keys.clear();
        }

        fn use_snapshots() -> bool {
            true
        }
    }*/
}

pub mod utils {
    use super::{ds::NodeId, messages::*};
    use std::net::{SocketAddr, ToSocketAddrs};
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::TcpStream;
    use tokio_serde::{formats::Bincode, Framed};
    use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};

    pub type Timestamp = i64;

    pub fn get_node_addr(
        cluster_name: &String,
        node: NodeId,
        is_local: bool,
    ) -> Result<SocketAddr, std::io::Error> {
        let dns_name: String = if is_local {
            format!("s{node}:800{node}")
        } else {
            format!("{cluster_name}-server-{node}.internal.zone.:800{node}")
        };
        let address = dns_name.to_socket_addrs()?.next().unwrap();
        Ok(address)
    }

    pub type RegistrationConnection = Framed<
        CodecFramed<TcpStream, LengthDelimitedCodec>,
        RegistrationMessage,
        RegistrationMessage,
        Bincode<RegistrationMessage, RegistrationMessage>,
    >;

    pub fn frame_registration_connection(stream: TcpStream) -> RegistrationConnection {
        let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
        Framed::new(length_delimited, Bincode::default())
    }

    pub type FromNodeConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ClusterMessage,
        (),
        Bincode<ClusterMessage, ()>,
    >;
    pub type ToNodeConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ClusterMessage,
        Bincode<(), ClusterMessage>,
    >;

    pub fn frame_cluster_connection(stream: TcpStream) -> (FromNodeConnection, ToNodeConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromNodeConnection::new(stream, Bincode::default()),
            ToNodeConnection::new(sink, Bincode::default()),
        )
    }

    // pub type ServerConnection = Framed<
    //     CodecFramed<TcpStream, LengthDelimitedCodec>,
    //     ServerMessage,
    //     ClientMessage,
    //     Bincode<ServerMessage, ClientMessage>,
    // >;

    pub type FromServerConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ServerMessage,
        (),
        Bincode<ServerMessage, ()>,
    >;

    pub type ToServerConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ClientMessage,
        Bincode<(), ClientMessage>,
    >;

    pub type FromClientConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ClientMessage,
        (),
        Bincode<ClientMessage, ()>,
    >;

    pub type ToClientConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ServerMessage,
        Bincode<(), ServerMessage>,
    >;

    pub fn frame_clients_connection(
        stream: TcpStream,
    ) -> (FromServerConnection, ToServerConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromServerConnection::new(stream, Bincode::default()),
            ToServerConnection::new(sink, Bincode::default()),
        )
    }

    // pub fn frame_clients_connection(stream: TcpStream) -> ServerConnection {
    //     let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
    //     Framed::new(length_delimited, Bincode::default())
    // }

    pub fn frame_servers_connection(
        stream: TcpStream,
    ) -> (FromClientConnection, ToClientConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromClientConnection::new(stream, Bincode::default()),
            ToClientConnection::new(sink, Bincode::default()),
        )
    }
}
