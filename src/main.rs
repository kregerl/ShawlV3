use std::{
    collections::HashSet,
    env::{self, VarError},
    ffi::OsString,
    net::Ipv4Addr,
    str::FromStr,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use chrono::{format, Local, SecondsFormat};
use consts::{AUTOMATIC_QUEUE_NAME, HEARTBEAT_EXCHANGE, IP_RANGE_EXCHANGE};
use env_logger::Builder;
use futures_lite::{future::block_on, StreamExt};
use gethostname::gethostname;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use log::{debug, info};
use rand::{seq::SliceRandom, thread_rng};
use std::io::Write;
use tokio::sync::RwLock;
use tokio_postgres::{Client, NoTls};

mod consts;

type SqlResult<T> = Result<T, tokio_postgres::Error>;

/// Generates a vec of shuffles IP addresses,
/// skippping all private & special IP addresses
fn generate_ip_ranges() -> Vec<String> {
    let first_octets: Vec<u8> = (1..=0xFE).collect();
    let second_octets: Vec<u8> = (1..=0xFE).collect();

    let mut ranges = Vec::new();
    for first_octet in first_octets {
        // Class A private IP OR
        // Loopback OR
        // Class D (224 - 239) OR
        // Class E (240-255)
        if first_octet == 10 || first_octet == 127 || first_octet >= 224 {
            continue;
        }

        for second_octet in &second_octets {
            // Class B private IP
            if first_octet == 172 && *second_octet >= 16 && *second_octet <= 31 {
                continue;
            }

            // Class B private APIPA range
            if first_octet == 169 && *second_octet == 254 {
                continue;
            }

            // Class C private IP
            if first_octet == 192 && *second_octet == 168 {
                continue;
            }
            ranges.push(format!("{}.{}.0.0/16", first_octet, second_octet));
        }
    }
    ranges.shuffle(&mut thread_rng());
    ranges
}

struct State {
    workers: RwLock<Vec<String>>,
    is_master_node: RwLock<bool>,
}

impl State {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            workers: RwLock::new(Vec::new()),
            is_master_node: RwLock::new(false),
        })
    }
}

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

    dotenv::dotenv().ok();

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

    let ip_range_write_channel = connection.create_channel().await.unwrap();
    let ip_range_read_channel = connection.create_channel().await.unwrap();

    let state = State::new();

    // let is_master_node = Arc::new(RwLock::new(false));

    tokio_scoped::scope(|scope| {
        // let is_master_node = is_master_node.clone();
        scope.spawn(async {
            match heartbeat(send_heartbeat_channel).await {
                Ok(_) => {}
                Err(e) => panic!("RabbitMq Error: {}", e),
            }
        });
        scope.spawn(async {
            match read_rabbit(read_heartbeat_channel, state.clone()).await {
                Ok(_) => {}
                Err(e) => panic!("RabbitMq Error: {}", e),
            }
        });
        scope.spawn(async {
            // let client = connect_to_db().await.unwrap();
            // let ranges = generate_ip_ranges();

            // execute_empty_statement(&client, include_sql!("create_scanned_range_table.sql")).await.unwrap();
            // if let Some(range) = ranges.iter().next() {
            //     let x = client.query(include_sql!("check_for_existing_range.sql"), &[&range]).await.unwrap();

            //     // If the IP already exists in the scanned_ip_ranges table
            //     if x.len() > 0 {
            //         info!("IP range already exists in database");
            //     } else {
            //         info!("IP range does not exists in database");
            //     }
            // }
            let hostname = gethostname::gethostname().into_string().unwrap();
            // tokio::time::sleep(Duration::from_secs(15)).await;

            let options = ExchangeDeclareOptions {
                durable: false,
                ..Default::default()
            };
            ip_range_write_channel
                .exchange_declare(
                    IP_RANGE_EXCHANGE,
                    lapin::ExchangeKind::Direct,
                    options,
                    FieldTable::default(),
                )
                .await
                .unwrap();

            let mut options = QueueDeclareOptions::default();
            options.exclusive = true;
            ip_range_read_channel
                .queue_declare(AUTOMATIC_QUEUE_NAME, options, FieldTable::default())
                .await
                .unwrap();

            ip_range_read_channel
                .queue_bind(
                    AUTOMATIC_QUEUE_NAME,
                    IP_RANGE_EXCHANGE,
                    &hostname,
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .unwrap();

            let mut basic_options = BasicConsumeOptions::default();
            basic_options.no_ack = true;
            let mut consumer = ip_range_read_channel
                .basic_consume(
                    AUTOMATIC_QUEUE_NAME,
                    &hostname,
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .unwrap();

            loop {
                tokio::time::sleep(Duration::from_secs(15)).await;
                debug!("----------------------------");
                debug!("Is master node: {}", state.is_master_node.read().await);
                debug!("Workers: {:#?}", state.workers.read().await);

                if *state.is_master_node.read().await {
                    loop {
                        debug!("Inside master node");

                        if !*state.is_master_node.read().await {
                            debug!("Breaking out of master node loop");
                            break;
                        }
                        let ip_range = "73.143.0.0/16";
                        let workers = state.workers.read().await;

                        for worker in workers
                            .iter()
                            .filter(|worker_hostname| **worker_hostname != hostname)
                        {
                            ip_range_write_channel
                                .basic_publish(
                                    IP_RANGE_EXCHANGE,
                                    worker,
                                    BasicPublishOptions::default(),
                                    ip_range.as_bytes(),
                                    BasicProperties::default().with_expiration("60000".into()),
                                )
                                .await
                                .unwrap();
                        }
                        tokio::time::sleep(Duration::from_secs(15)).await;
                    }
                } else {
                    debug!("Not a master node, looping");
                    loop {
                        if let Ok(Some(delivery)) =
                            tokio::time::timeout(Duration::from_secs(1), consumer.next())
                                .await
                        {
                            let delivery = delivery.expect("error in consumer");
                            delivery.ack(BasicAckOptions::default()).await.expect("ack");
                            let ip_range = String::from_utf8(delivery.data).unwrap();
                            debug!("Worker received ip range: {}", ip_range);
                        }
                        if !*state.is_master_node.read().await {
                            break;
                        }
                    }
                }
            }

            // loop {
            //     debug!("Is master node: {}", state.is_master_node.read().await);
            //     debug!("Workers: {:#?}", state.workers.read().await);

            //     if *state.is_master_node.read().await {
            //         let options = ExchangeDeclareOptions {
            //             durable: false,
            //             ..Default::default()
            //         };
            //         ip_range_write_channel
            //             .exchange_declare(
            //                 IP_RANGE_EXCHANGE,
            //                 lapin::ExchangeKind::Direct,
            //                 options,
            //                 FieldTable::default(),
            //             )
            //             .await
            //             .unwrap();

            //         loop {
            //             debug!("Inside master node inner loop");
            //             tokio::time::sleep(Duration::from_secs(15)).await;
            //             let ip_range = "73.143.0.0/16";
            //             let workers = state.workers.read().await;

            //             if !*state.is_master_node.read().await {
            //                 break;
            //             }

            //             for worker in workers
            //                 .iter()
            //                 .filter(|worker_hostname| **worker_hostname != hostname)
            //             {
            //                 ip_range_write_channel
            //                     .basic_publish(
            //                         IP_RANGE_EXCHANGE,
            //                         worker,
            //                         BasicPublishOptions::default(),
            //                         ip_range.as_bytes(),
            //                         BasicProperties::default().with_expiration("60000".into()),
            //                     )
            //                     .await
            //                     .unwrap();
            //             }
            //         }
            //     } else {
            //         debug!("Not the master node");
            //         // Not master
            //         let mut options = ExchangeDeclareOptions::default();
            //         options.durable = false;
            //         ip_range_read_channel
            //             .exchange_declare(
            //                 IP_RANGE_EXCHANGE,
            //                 lapin::ExchangeKind::Direct,
            //                 options,
            //                 FieldTable::default(),
            //             )
            //             .await
            //             .unwrap();

            //         let mut options = QueueDeclareOptions::default();
            //         options.exclusive = true;
            //         ip_range_read_channel
            //             .queue_declare(AUTOMATIC_QUEUE_NAME, options, FieldTable::default())
            //             .await
            //             .unwrap();

            //         ip_range_read_channel
            //             .queue_bind(
            //                 AUTOMATIC_QUEUE_NAME,
            //                 IP_RANGE_EXCHANGE,
            //                 &hostname,
            //                 QueueBindOptions::default(),
            //                 FieldTable::default(),
            //             )
            //             .await
            //             .unwrap();

            //         let mut basic_options = BasicConsumeOptions::default();
            //         basic_options.no_ack = true;
            //         let mut consumer = ip_range_read_channel
            //             .basic_consume(
            //                 AUTOMATIC_QUEUE_NAME,
            //                 "my_consumer",
            //                 BasicConsumeOptions::default(),
            //                 FieldTable::default(),
            //             )
            //             .await
            //             .unwrap();

            //         while let Some(delivery) = consumer.next().await {
            //             let delivery = delivery.expect("error in consumer");
            //             let range = String::from_utf8(delivery.data).unwrap();
            //             info!("Worker node got ip range: {:#?}", range);
            //         }
            //     }
            //     tokio::time::sleep(Duration::from_secs(15)).await;
            // }
            // loop {
            //     tokio::time::sleep(Duration::from_secs(15)).await;
            //     info!("Is master node: {}", state.is_master_node.read().await);
            //     info!("Workers: {:#?}", state.workers.read().await);
            // }
        });
    });
}

async fn execute_empty_statement(client: &Client, query: &str) -> SqlResult<()> {
    let stmt = client.prepare(query).await?;
    client.execute(&stmt, &[]).await?;
    Ok(())
}

#[macro_export]
macro_rules! include_sql {
    ($file:expr $(,)?) => {{
        include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/sql/", $file))
    }};
}

async fn connect_to_db() -> Result<Client, VarError> {
    let connect_config = &format!(
        "host={} user={} password={} dbname={}",
        std::env::var("DB_HOST")?,
        std::env::var("DB_USERNAME")?,
        std::env::var("DB_PASSWORD")?,
        std::env::var("DB_NAME")?,
    );

    let (client, connection) = tokio_postgres::connect(&connect_config, NoTls)
        .await
        .unwrap();

    let _ = tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection Error: {}", e);
        }
    });

    Ok(client)
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

async fn read_rabbit(channel: Channel, state: Arc<State>) -> lapin::Result<()> {
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
                let mut is_master_node = state.is_master_node.write().await;
                *is_master_node = if hostname == OsString::from(node_hostname) {
                    true
                } else {
                    false
                };
            }

            let mut workers = state.workers.write().await;
            *workers = nodes;

            set.clear();
            start = Instant::now();
        }
    }
}
