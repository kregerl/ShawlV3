use std::{
    collections::HashSet,
    ffi::OsString,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use chrono::Local;
use consts::{AUTOMATIC_QUEUE_NAME, HEARTBEAT_EXCHANGE};
use env_logger::Builder;
use futures_lite::StreamExt;
use gethostname::gethostname;
use lapin::{
    options::{
        BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    }, types::FieldTable, BasicProperties, Channel, Connection, ConnectionProperties
};
use log::{debug, info};
use std::io::Write;
use tokio::sync::RwLock;

mod consts;

#[tokio::main]
async fn main() {
    // let hostname = "73.143.73.63";
    // let port = 25565;
    // let mut stream = TcpStream::connect((hostname, port)).await.unwrap();
    // let pong = ping(&mut stream, hostname, port).await.expect("Cannot ping server");
    // println!("Ping result: {:?}", pong);

    // TODO: Implement ACK for all messages from RabbitMq
    // TODO: Implement the master node's work
    //      - Generate random IPs and send to RabbitMq queue
    //      - Read from another queue containing all the worker's results
    //      - Push results from workers to postgresql 

    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "[{}] [{}] {} - {}",
                Local::now().format("%y-%m-%d %H:%M:%S"),
                gethostname().into_string().unwrap(),
                record.level(),
                record.args()
            )
        })
        .filter(None, log::LevelFilter::Debug)
        .init();

    let addr = "amqp://192.168.1.253:5672";
    let connection = Connection::connect(addr, ConnectionProperties::default())
        .await
        .unwrap();

    let send_heartbeat_channel = connection.create_channel().await.unwrap();
    let read_heartbeat_channel = connection.create_channel().await.unwrap();

    let is_master_node = Arc::new(RwLock::new(false));

    tokio_scoped::scope(|scope| {
        // let is_master_node = is_master_node.clone();
        scope.spawn(async {
            match heartbeat(send_heartbeat_channel).await {
                Ok(_) => {}
                Err(e) => panic!("RabbitMq Error: {}", e),
            }
        });
        scope.spawn(async {
            match read_rabbit(read_heartbeat_channel, is_master_node.clone()).await {
                Ok(_) => {}
                Err(e) => panic!("RabbitMq Error: {}", e),
            }
        });
        scope.spawn(async {
            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                info!("Is master node: {}", is_master_node.read().await);
            }
        });
    });
}

async fn heartbeat(channel: Channel) -> lapin::Result<()> {
    let options = ExchangeDeclareOptions {
        durable: false,
        ..Default::default()
    };
    channel
        .exchange_declare(
            HEARTBEAT_EXCHANGE,
            lapin::ExchangeKind::Fanout,
            options,
            FieldTable::default(),
        )
        .await?;
    let hostname = gethostname::gethostname();
    loop {
        channel
            .basic_publish(
                HEARTBEAT_EXCHANGE,
                "",
                BasicPublishOptions::default(),
                hostname.as_encoded_bytes(),
                BasicProperties::default().with_expiration("60000".into()),
            )
            .await
            .unwrap();
        thread::sleep(Duration::from_secs(10));
    }
}

async fn read_rabbit(channel: Channel, is_master_node: Arc<RwLock<bool>>) -> lapin::Result<()> {
    let mut options = ExchangeDeclareOptions::default();
    options.durable = false;
    channel
        .exchange_declare(
            HEARTBEAT_EXCHANGE,
            lapin::ExchangeKind::Fanout,
            options,
            FieldTable::default(),
        )
        .await?;

    let mut options = QueueDeclareOptions::default();
    options.exclusive = true;
    channel
        .queue_declare(AUTOMATIC_QUEUE_NAME, options, FieldTable::default())
        .await?;

    channel
        .queue_bind(
            AUTOMATIC_QUEUE_NAME,
            HEARTBEAT_EXCHANGE,
            "",
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let mut basic_options = BasicConsumeOptions::default();
    basic_options.no_ack = true;
    let mut consumer = channel
        .basic_consume(
            AUTOMATIC_QUEUE_NAME,
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    let hostname = gethostname();
    let mut start = Instant::now();
    let mut set: HashSet<String> = HashSet::new();
    loop {
        if let Some(delivery) = consumer.next().await {
            let delivery = delivery.expect("error in consumer");
            let hostname = String::from_utf8(delivery.data).unwrap();
            set.insert(hostname);
        }
        if start.elapsed().as_secs() > 15 {
            debug!("Clearing");
            let node_set = set.clone();
            let mut nodes = node_set.into_iter().collect::<Vec<_>>();
            nodes.sort();
            debug!("Nodes: {:?}", nodes);
            if let Some(node_hostname) = nodes.last() {
                debug!("Master Node is: {}", node_hostname);
                let mut is_master_node = is_master_node.write().await;
                *is_master_node = if hostname == OsString::from(node_hostname) {
                    true
                } else {
                    false
                };
            }

            set.clear();
            start = Instant::now();
        }
    }
}