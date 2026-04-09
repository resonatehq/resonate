use std::{fmt::Display, time::Duration};

use resonate::prelude::*;

#[resonate::function(name = "slow")]
async fn slow(delay: u64) -> Result<()> {
    tokio::time::sleep(Duration::from_secs(delay)).await;
    Ok(())
}

// A "hello world" workflow that calls both leaf functions
// and combines the results.
#[resonate::function]
async fn workflow1(ctx: &Context) -> Result<String> {
    let (f1, f2, f3, f4) = tokio::join!(
        ctx.rpc::<()>("slow", 2),
        ctx.rpc::<()>("slow", 2),
        ctx.rpc::<()>("slow", 2),
        ctx.rpc::<()>("slow", 2),
    );

    f1?;
    f2?;
    f3?;
    f4?;
    Ok("Done workflow1".to_string())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct MyType {
    x: String,
    y: String,
}

impl Display for MyType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "x:{}, y:{}", self.x, self.y)
    }
}

#[resonate::function]
async fn par_workflow(ctx: &Context) -> Result<MyType> {
    ctx.rpc::<()>("slow", 2).await?;
    let f1 = ctx.rpc::<()>("slow", 2).spawn().await?;
    let f2 = ctx.rpc::<()>("slow", 2).spawn().await?;
    let f3 = ctx.rpc::<()>("slow", 2).spawn().await?;

    let h = ctx.run(slow, 5).spawn().await?;
    h.await?;

    f1.await?;
    f2.await?;
    f3.await?;

    Ok(MyType {
        x: "Hello".to_string(),
        y: "World".to_string(),
    })
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let resonate = Resonate::new(ResonateConfig {
        url: Some("http://localhost:8001".to_string()),
        ..Default::default()
    });

    // Register all functions.
    resonate.register(slow).unwrap();
    resonate.register(par_workflow).unwrap();
    resonate.register(workflow1).unwrap();

    // Call the workflow function.
    let result: MyType = resonate
        .run("workflow-y", par_workflow, ())
        .await
        .expect("workflow failed");

    println!("{}", result);

    println!("done!")
}
