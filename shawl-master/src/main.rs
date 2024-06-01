use chrono::Local;
use env_logger::Builder;
use futures_lite::stream::StreamExt;
use gethostname::gethostname;
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, ExchangeDeclareOptions,
        QueueBindOptions, QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties,
};
use log::debug;
use shawl_common::{
    ScanResults, AUTOMATIC_QUEUE_NAME, HEARTBEAT_EXCHANGE, IP_RANGE_EXCHANGE, RESULTS_EXCHANGE,
};
use std::{
    collections::{HashMap, HashSet},
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() {
    dotenv::dotenv().ok();

    Builder::new()
        .format(|buf, record| {
            writeln!(
                buf,
                "[{}] [{}] [{:?}] {} - {}",
                Local::now().format("%y-%m-%d %H:%M:%S"),
                gethostname().into_string().unwrap(),
                std::thread::current().id(),
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
    // Map of worker node hostname to whether or not it has a job.
    let workers: Arc<RwLock<HashMap<String, bool>>> = Arc::new(RwLock::new(HashMap::new()));

    let heartbeat_read_channel = connection.create_channel().await.unwrap();
    let results_read_channel = connection.create_channel().await.unwrap();
    let ip_range_write_channel = connection.create_channel().await.unwrap();

    tokio_scoped::scope(|scope| {
        scope.spawn(async {
            match read_rabbit(heartbeat_read_channel, workers.clone()).await {
                Ok(_) => {}
                Err(e) => panic!("RabbitMq Error: {}", e),
            }
        });
        scope.spawn(async {
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

            loop {
                debug!("Inside Loop!");
                let ip_range = "73.143.73.0/24";

                let mut occupied_workers: Vec<String> = Vec::new();
                for (worker, is_open) in workers.read().await.iter() {
                    debug!("{}: {}", worker, is_open);
                    if !is_open {
                        continue;
                    }

                    ip_range_write_channel
                        .basic_publish(
                            IP_RANGE_EXCHANGE,
                            &worker,
                            BasicPublishOptions::default(),
                            ip_range.as_bytes(),
                            BasicProperties::default().with_expiration("60000".into()),
                        )
                        .await
                        .unwrap();

                    occupied_workers.push(worker.clone());
                }
                debug!("Occupied workers: {:#?}", occupied_workers);
                for worker in occupied_workers {
                    if let Some(is_open) = workers.write().await.get_mut(&worker) {
                        debug!("Should set to false?");
                        *is_open = false;
                    }
                }
                tokio::time::sleep(Duration::from_secs(15)).await;
            }
        });
        scope.spawn(async {
            results_read_channel
                .exchange_declare(
                    RESULTS_EXCHANGE,
                    lapin::ExchangeKind::Fanout,
                    ExchangeDeclareOptions {
                        durable: false,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .unwrap();

            results_read_channel
                .queue_declare(
                    AUTOMATIC_QUEUE_NAME,
                    QueueDeclareOptions {
                        exclusive: true,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .unwrap();

            results_read_channel
                .queue_bind(
                    AUTOMATIC_QUEUE_NAME,
                    &RESULTS_EXCHANGE,
                    "",
                    QueueBindOptions::default(),
                    FieldTable::default(),
                )
                .await
                .unwrap();

            let mut consumer = results_read_channel
                .basic_consume(
                    AUTOMATIC_QUEUE_NAME,
                    "",
                    BasicConsumeOptions::default(),
                    FieldTable::default(),
                )
                .await
                .unwrap();

            loop {
                if let Ok(Some(delivery)) =
                    tokio::time::timeout(Duration::from_secs(1), consumer.next()).await
                {
                    debug!("Got delivery");
                    let delivery = delivery.unwrap();
                    delivery.ack(BasicAckOptions::default()).await.expect("ack");
                    let x = serde_json::from_slice::<ScanResults>(&delivery.data)
                        .expect("Error deserializing scan results");
                    debug!("Received scan results: {:#?}", x);
                    if let Some(is_open) = workers.write().await.get_mut(&x.id) {
                        *is_open = true;
                    }
                }

            }
            // while let Some(delivery) = consumer.next().await {
                
            // }
        });
    });
}

async fn read_rabbit(
    channel: Channel,
    workers: Arc<RwLock<HashMap<String, bool>>>,
) -> lapin::Result<()> {
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

    let mut consumer = channel
        .basic_consume(
            AUTOMATIC_QUEUE_NAME,
            "my_consumer",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;
    // jeFF WAS hERE
    let mut start = Instant::now();
    let mut set: HashSet<String> = HashSet::new();
    loop {
        if let Ok(Some(delivery)) =
            tokio::time::timeout(Duration::from_secs(1), consumer.next()).await
        {
            // if let Some(delivery) = consumer.next().await {
            let delivery = delivery.expect("error in consumer");
            delivery.ack(BasicAckOptions::default()).await?;
            let hostname = String::from_utf8(delivery.data).unwrap();
            set.insert(hostname);
        }
        if start.elapsed().as_secs() > 15 {
            debug!("Clearing");
            let node_set = set.clone();
            let mut nodes = node_set.into_iter().collect::<Vec<_>>();
            nodes.sort();
            debug!("Nodes: {:?}", nodes);

            let mut workers = workers.write().await;
            for node in nodes {
                if workers.get(&node).is_none() {
                    workers.insert(node, true);
                }
            }

            set.clear();
            start = Instant::now();
        }
    }
}
