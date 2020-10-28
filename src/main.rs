#[macro_use]
extern crate anyhow;

#[macro_use]
extern crate thiserror;

use std::time::Duration;
use std::time::Instant;

use ::futures::prelude::*;
use ::tokio::io::AsyncWriteExt;
use ::tokio::io::BufWriter;

mod amqp_stream;
mod util;

#[tokio::main]
async fn main() {
    let _ = dotenv::dotenv();
    let _ = pretty_env_logger::init_timed();

    run().await.expect("Failure")
}

async fn run() -> Result<(), ::anyhow::Error> {
    let start_time = Instant::now();

    let amqp_uri =
        std::env::var("AMQP_URI").unwrap_or("amqp://guest:guest@127.0.0.1:5672/".to_owned());

    let app_id = std::env::var("APP_ID").unwrap_or("amqp-dump".to_owned());

    let bind_src_exchange =
        std::env::var("BIND_SRC_EXCHANGE").unwrap_or("v2.platform-api".to_owned());

    let bind_routing_key = std::env::var("BIND_ROUTING_KEY").unwrap_or("#".to_owned());

    let conns_count: usize = std::env::var("CONNS_COUNT")
        .unwrap_or("1".to_owned())
        .parse()?;

    let tick_interval: Duration =
        ::parse_duration::parse(&std::env::var("TICK_INTERVAL").unwrap_or("10s".to_owned()))?;

    let column_separator = std::env::var("COLUMN_SEPARATOR").unwrap_or(",".to_owned());
    let dump_routing_key: bool = std::env::var("DUMP_CONTENT").unwrap_or("1".to_owned()) == "1";
    let dump_content: bool = std::env::var("DUMP_CONTENT").ok() == Some("1".to_owned());
    let dump_headers: Vec<lapin::types::ShortString> = std::env::var("DUMP_HEADERS")
        .unwrap_or("".to_owned())
        .split(",")
        .filter(|s| !s.is_empty())
        .map(|s| s.to_owned())
        .map(Into::into)
        .collect();

    let amqp_stream = amqp_stream::amqp_stream(
        &amqp_uri,
        &app_id,
        &bind_src_exchange,
        &bind_routing_key,
        conns_count,
    )
    .await?;

    let consumers = amqp_stream.consumers;
    let consumers = stream::select_all(consumers).map_ok(Event::Delivery);

    let ticks = ::tokio::time::interval(tick_interval)
        .map(Into::into)
        .map(Event::Tick)
        .map(Ok);

    let events = stream::select(consumers, ticks);
    ::futures::pin_mut!(events);

    let stdout = ::tokio::io::stdout();
    let mut stdout = BufWriter::new(stdout);

    let stderr = ::tokio::io::stderr();
    let mut stderr = BufWriter::new(stderr);

    let delivery_format = DeliveryFormat {
        start_time,
        column_separator,
        dump_routing_key,
        dump_headers,
        dump_content,
    };

    while let Some(event) = events.next().await {
        let event = event?;

        match event {
            Event::Tick(instant) => {
                let _ = stdout.flush().await?;
                let _ = stderr.write(format!("\ntick\n").as_bytes()).await?;
                let _ = stderr.flush().await?;
            }
            Event::Delivery((_channel, delivery)) => {
                delivery_format.write(&mut stdout, delivery).await?;
                // let _ = stdout.write(".\n".as_bytes()).await?;
            }
        }
    }

    Ok(())
}

struct DeliveryFormat {
    start_time: Instant,
    column_separator: String,
    dump_routing_key: bool,
    dump_content: bool,
    dump_headers: Vec<lapin::types::ShortString>,
}
impl DeliveryFormat {
    pub async fn write<W: ::tokio::io::AsyncWrite + std::marker::Unpin>(
        &self,
        w: &mut BufWriter<W>,
        delivery: lapin::message::Delivery,
    ) -> Result<(), anyhow::Error> {
        let now = self.start_time.elapsed();

        w.write(format!("{:>6}.{:0>6}", now.as_secs(), now.subsec_micros()).as_bytes())
            .await?;
        w.write(self.column_separator.as_bytes()).await?;

        if self.dump_routing_key {
            w.write(delivery.routing_key.as_str().as_bytes()).await?;
            w.write(self.column_separator.as_bytes()).await?;
        }
        for header in &self.dump_headers {
            let value = if let Some(value) = delivery
                .properties
                .headers()
                .as_ref()
                .and_then(|hs| hs.inner().get(header))
            {
                match value {
                    lapin::types::AMQPValue::ShortString(s) => s.as_str(),
                    lapin::types::AMQPValue::LongString(s) => s.as_str(),
                    _ => "<NOT-STR>",
                }
            } else {
                "<NONE>"
            };
            w.write(value.as_bytes()).await?;
            w.write(self.column_separator.as_bytes()).await?;
        }
        if self.dump_content {
            w.write(hex::encode(&delivery.data).as_bytes()).await?;
        }
        w.write("\n".as_bytes()).await?;
        Ok(())
    }
}

enum Event {
    Tick(Instant),
    Delivery((lapin::Channel, lapin::message::Delivery)),
}
