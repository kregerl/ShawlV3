use chrono::Local;
use craftping::tokio::ping;
use env_logger::Builder;
use futures_util::{stream, StreamExt};
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
use shawl_common::{AUTOMATIC_QUEUE_NAME, HEARTBEAT_EXCHANGE, IP_RANGE_EXCHANGE};
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
                    let x = stream::iter(ip_range.hosts())
                        .for_each_concurrent(CONCURRENCY, |addr| scan_ports(addr))
                        .await;
                }
            }
        });
    });
}

async fn scan_ports(addr: Ipv4Addr) {
    let ports = vec![25560];

    let address = addr.to_string();
    for port in ports {
        debug!("Scanning addr: {}:{}", address, port);
        if let Ok(Ok(mut stream)) = tokio::time::timeout(
            Duration::from_secs(5),
            TcpStream::connect((address.clone(), port)),
        )
        .await
        {
            let pong = ping(&mut stream, &address, port)
                .await
                .expect("Cannot ping server");
            debug!("Ping result: {:?}", pong);
        }
    }
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
