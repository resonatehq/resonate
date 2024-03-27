import { Resonate, Context } from "@resonatehq/sdk";

const resonate = new Resonate();

resonate.register("app", (ctx: Context) => {
  return "Hello World";
});

async function main() {
  await resonate.run("app", "app.1");
}

main();
