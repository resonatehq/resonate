use std::time::Duration;

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

#[resonate::function]
async fn par_workflow(ctx: &Context) -> Result<String> {
    let f = ctx.rpc::<()>("slow", 2).spawn().await?;
    let f1 = ctx.rpc::<()>("slow", 2).spawn().await?;
    let f2 = ctx.rpc::<()>("slow", 2).spawn().await?;
    let f3 = ctx.rpc::<()>("slow", 2).spawn().await?;

    f.await?;
    f1.await?;
    f2.await?;
    f3.await?;

    Ok("Done par_workflow.".to_string())
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let resonate = Resonate::local(None);

    // Register all functions.
    resonate.register(slow).unwrap();
    resonate.register(par_workflow).unwrap();
    resonate.register(workflow1).unwrap();

    // Call the shout function.
    let result: String = resonate
        .run("workflow", par_workflow, (), None)
        .await
        .expect("worflow failed");
    println!("{}", result);

    println!("done!")
}
