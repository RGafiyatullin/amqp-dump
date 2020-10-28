use super::*;

use ::lapin::options::BasicConsumeOptions;
use ::lapin::options::ExchangeBindOptions;
use ::lapin::options::ExchangeDeclareOptions;
use ::lapin::options::QueueBindOptions;
use ::lapin::options::QueueDeclareOptions;
use ::lapin::Channel;
use ::lapin::Connection;
use ::lapin::Consumer;
use ::lapin::ExchangeKind;

pub struct AmqpStream {
    connections: Vec<Connection>,
    channels: Vec<Channel>,
    pub consumers: Vec<Consumer>,
}

pub async fn amqp_stream(
    amqp_uri: &str,
    app_id: &str,
    src_exchange: &str,
    routing_key: &str,
    conns_count: usize,
) -> Result<AmqpStream, ::anyhow::Error> {
    let app_exchange = format!("{}", app_id);
    let exchange_kind = ExchangeKind::Custom("x-random".to_owned());
    let exchange_declare_opts: ExchangeDeclareOptions = ExchangeDeclareOptions {
        auto_delete: true,
        ..Default::default()
    };
    let exchange_bind_opts: ExchangeBindOptions = Default::default();
    let queue_declare_opts: QueueDeclareOptions = QueueDeclareOptions {
        exclusive: true,
        auto_delete: true,
        ..Default::default()
    };
    let queue_bind_opts: QueueBindOptions = Default::default();
    let basic_consume_opts: BasicConsumeOptions = BasicConsumeOptions {
        no_ack: true,
        exclusive: true,
        ..Default::default()
    };

    let mut connections = Vec::new();
    let mut channels = Vec::new();
    let mut consumers = Vec::new();

    for idx in 0..conns_count {
        let queue_name = format!("{}_{}", app_id, idx);

        let connection = Connection::connect(amqp_uri, Default::default()).await?;
        let channel = connection.create_channel().await?;

        let () = channel
            .exchange_declare(
                &app_exchange,
                exchange_kind.clone(),
                exchange_declare_opts.clone(),
                Default::default(),
            )
            .await?;
        let () = channel
            .exchange_bind(
                &app_exchange,
                src_exchange,
                routing_key,
                exchange_bind_opts.clone(),
                Default::default(),
            )
            .await?;
        let _queue_created = channel
            .queue_declare(&queue_name, queue_declare_opts.clone(), Default::default())
            .await?;
        let () = channel
            .queue_bind(
                &queue_name,
                &app_exchange,
                "",
                queue_bind_opts.clone(),
                Default::default(),
            )
            .await?;

        let consumer = channel
            .basic_consume(
                &queue_name,
                &queue_name,
                basic_consume_opts.clone(),
                Default::default(),
            )
            .await?;

        connections.push(connection);
        channels.push(channel);
        consumers.push(consumer);
    }

    Ok(AmqpStream {
        connections,
        channels,
        consumers,
    })
}
