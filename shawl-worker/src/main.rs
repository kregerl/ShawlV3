use chrono::Local;
use craftping::{tokio::ping, Response};
use env_logger::Builder;
use futures_util::{stream, FutureExt, StreamExt};
use gethostname::gethostname;
use ipnet::Ipv4Net;
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
use std::{io::Write, net::Ipv4Addr, str::FromStr, thread, time::Duration};
use tokio::net::TcpStream;

const CONCURRENCY: usize = 4;

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

    let heartbeat_write_channel = connection.create_channel().await.unwrap();
    let ip_range_read_channel = connection.create_channel().await.unwrap();
    let results_write_channel = connection.create_channel().await.unwrap();

    tokio_scoped::scope(|scope| {
        scope.spawn(async {
            match heartbeat(heartbeat_write_channel).await {
                Ok(_) => {}
                Err(e) => panic!("RabbitMq Error: {}", e),
            }
        });
        scope.spawn(async {
            let hostname = gethostname::gethostname().into_string().unwrap();
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

            results_write_channel
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
            // jEFF WAS HERE
            loop {
                if let Ok(Some(delivery)) =
                    tokio::time::timeout(Duration::from_secs(1), consumer.next()).await
                {
                    let delivery = delivery.expect("error in consumer");
                    delivery.ack(BasicAckOptions::default()).await.expect("ack");
                    let ip_range_str = String::from_utf8(delivery.data).unwrap();
                    debug!("Worker received ip range: {}", ip_range_str);

                    let ip_range = Ipv4Net::from_str(&ip_range_str).unwrap();

                    let mut futures = Vec::new();
                    for host in ip_range.hosts() {
                        futures.push(scan_ports(host));
                    }
                    let responses = stream::iter(futures)
                        .buffer_unordered(CONCURRENCY)
                        .collect::<Vec<Vec<Response>>>()
                        .await
                        .into_iter()
                        .flatten()
                        .collect::<Vec<Response>>();

                    let hostname = gethostname::gethostname().into_string().unwrap();
                    let payload = ScanResults {
                        id: hostname,
                        servers: responses,
                    };
                    debug!(
                        "Done scanning, sending to the results exchange: {:#?}",
                        payload
                    );
                    let bytes =
                        serde_json::to_vec(&payload).expect("Error converting payload to bytes");
                    let x = results_write_channel
                        .basic_publish(
                            RESULTS_EXCHANGE,
                            "",
                            BasicPublishOptions::default(),
                            &bytes,
                            BasicProperties::default(),
                        )
                        .await
                        .unwrap();
                    debug!("Done sending: {:#?}", x);
                }
            }
        });
    });
}

async fn scan_ports(addr: Ipv4Addr) -> Vec<Response> {
    let ports = vec![25560];

    let address = addr.to_string();
    let mut responses = Vec::new();
    for port in ports {
        debug!("Scanning addr: {}:{}", address, port);
        if let Ok(Ok(mut stream)) = tokio::time::timeout(
            Duration::from_secs(5),
            TcpStream::connect((address.clone(), port)),
        )
        .await
        {
            let pong = ping(&mut stream, &address, port).await;
            debug!("Ping result: {:?}", pong);
            if let Ok(result) = pong {
                responses.push(result);
            }
        }
    }
    responses
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
        let x = channel
            .basic_publish(
                HEARTBEAT_EXCHANGE,
                "",
                BasicPublishOptions::default(),
                hostname.as_encoded_bytes(),
                BasicProperties::default().with_expiration("60000".into()),
            )
            .await?;
        debug!("Published confirm: {:#?}", x);
        thread::sleep(Duration::from_secs(10));
    }
}
