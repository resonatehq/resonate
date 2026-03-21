use resonate::prelude::*;
use tokio::io::AsyncWriteExt;

// A simple "hello world" leaf function that takes a name and returns a greeting.
#[resonate::function(name = "hello")]
async fn hello(name: String) -> Result<String> {
    let mut stdout = tokio::io::stdout();
    stdout
        .write_all(format!("hello {name} from tokio::stdout\n").as_bytes())
        .await?;
    Ok(format!("Hello, {}!", name))
}

// A second leaf function that adds an exclamation.
#[resonate::function]
async fn shout(message: String) -> Result<String> {
    Ok(message.to_uppercase())
}

// A "hello world" workflow that calls both leaf functions
// and combines the results.
#[resonate::function]
async fn hello_workflow(ctx: &Context, names: (String, String)) -> Result<String> {
    let f = ctx.begin_rpc::<String>("hello", &names.0).await;
    let greeting2: String = ctx.run(Hello, names.1).await?;
    let greeting1: String = f.await?;
    let shouted: String = ctx
        .run(Shout, format!("{} and {}", greeting1, greeting2))
        .await?;
    Ok(shouted)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let resonate = Resonate::local(None);

    // Register all functions.
    resonate.register(Hello);
    resonate.register(Shout);
    resonate.register(HelloWorkflow);

    // Call the shout function.
    let result: String = resonate
        .run("shout-1", Shout, "hello world".to_string(), None)
        .await
        .expect("shout failed");
    println!("{}", result);

    // Call the workflow that invokes hello twice and shouts the result.
    let result: String = resonate
        .run(
            "hello-workflow-1",
            HelloWorkflow,
            ("Alice".to_string(), "Bob".to_string()),
            None,
        )
        .await
        .expect("hello_workflow failed");
    println!("{}", result);
    println!("done!")
}
