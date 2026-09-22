// Local host for the streaming research UI (Supabase edge can't serve HTML).
//   GET  /       → the page (subscribes to the origin channel, tiles streams)
//   POST /start  → resonate.invoke('research', [question]) via the pooler
// The page talks to the REAL remote Supabase for Realtime + the deployed
// streaming `research` function; only the page hosting + start are local.
import postgres from "npm:postgres";

const SB_URL = Deno.env.get("SUPABASE_URL")!;
const ANON = Deno.env.get("SUPABASE_ANON_KEY")!;
const TARGET = Deno.env.get("RESEARCH_URL")!;
const sql = postgres(Deno.env.get("DATABASE_URL")!, { prepare: false });

Deno.serve({ port: 8890 }, async (req) => {
  const u = new URL(req.url);
  if (req.method === "POST" && u.pathname === "/start") {
    try {
      const { id, question } = await req.json();
      await sql`select resonate.invoke(${id}, 'research',
                  ${sql.json([question])}, ${TARGET})`;
      return json({ ok: true });
    } catch (e) {
      return json({ error: String(e) }, 500);
    }
  }
  return new Response(page(), {
    headers: { "content-type": "text/html; charset=utf-8" },
  });
});

const json = (b: unknown, status = 200) =>
  new Response(JSON.stringify(b), {
    status,
    headers: { "content-type": "application/json" },
  });

function page() {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Resonate · Research</title>
<style>
  :root{
    --bg:#0a0c11; --panel:#12151d; --tile:#161a24;
    --bd:#232838; --tx:#e7eaf0; --mut:#828aa0;
    --cy:#38bdf8; --gr:#34d399; --vi:#a78bfa; --rd:#f87171;
  }
  *{box-sizing:border-box}
  html,body{margin:0;height:100%}
  body{
    background:
      radial-gradient(1100px 520px at 80% -10%, rgba(56,189,248,.10), transparent 60%),
      radial-gradient(900px 480px at 0% 0%, rgba(167,139,250,.08), transparent 55%),
      var(--bg);
    color:var(--tx);
    font:15px/1.55 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
    -webkit-font-smoothing:antialiased;
  }
  .wrap{max-width:1120px;margin:0 auto;padding:40px 22px 80px}
  header{margin-bottom:26px}
  h1{margin:0;font-size:26px;letter-spacing:-.02em;font-weight:650}
  h1 .d{color:var(--cy)}
  .sub{color:var(--mut);margin-top:6px;font-size:14px;max-width:760px}
  form{display:flex;gap:10px;margin:22px 0 30px}
  input{
    flex:1;background:var(--panel);border:1px solid var(--bd);color:var(--tx);
    border-radius:12px;padding:14px 16px;font-size:15px;outline:none;
    transition:border-color .15s, box-shadow .15s;
  }
  input:focus{border-color:var(--cy);box-shadow:0 0 0 3px rgba(56,189,248,.14)}
  input::placeholder{color:#5a6178}
  button{
    background:linear-gradient(180deg,#3aa0ff,#2b7fe0);border:0;color:#fff;
    font-weight:600;font-size:15px;border-radius:12px;padding:0 22px;cursor:pointer;
    transition:filter .15s, transform .05s;
  }
  button:hover{filter:brightness(1.08)} button:active{transform:translateY(1px)}

  .board{
    background:var(--panel);border:1px solid var(--bd);border-radius:16px;
    padding:16px 16px 18px;margin-bottom:18px;animation:rise .25s ease both;
  }
  .bh{display:flex;align-items:baseline;gap:12px;justify-content:space-between;margin-bottom:14px}
  .q{font-weight:600;font-size:16px;letter-spacing:-.01em}
  .meta{display:flex;align-items:center;gap:10px;flex:0 0 auto}
  .chip{font:12px/1 ui-monospace,SFMono-Regular,Menlo,monospace;color:var(--mut);
    background:#0e1119;border:1px solid var(--bd);padding:5px 8px;border-radius:7px}
  .bs{font-size:12.5px;color:var(--cy);min-width:64px;text-align:right}
  .bs.done{color:var(--gr)} .bs.err{color:var(--rd)}

  .tiles{display:grid;grid-template-columns:repeat(auto-fill,minmax(258px,1fr));gap:12px}
  .tile{
    background:var(--tile);border:1px solid var(--bd);border-radius:12px;
    overflow:hidden;display:flex;flex-direction:column;animation:pop .22s ease both;
    box-shadow:0 0 0 1px rgba(56,189,248,.10);transition:box-shadow .2s,border-color .2s;
  }
  .tile.done{box-shadow:none;border-color:var(--bd)}
  .tile.report{grid-column:1/-1;box-shadow:0 0 0 1px rgba(167,139,250,.16)}
  .tile.report.done{box-shadow:none}
  .th{display:flex;align-items:center;gap:8px;padding:10px 12px;border-bottom:1px solid var(--bd);
    background:linear-gradient(180deg,rgba(255,255,255,.02),transparent)}
  .lbl{font-size:13px;font-weight:550;color:#cfd5e4;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
  .tile.report .lbl{color:var(--vi)}
  .dot{width:9px;height:9px;border-radius:50%;background:var(--cy);flex:0 0 auto;animation:pulse 1.4s ease-in-out infinite}
  .tile.report .dot{background:var(--vi)}
  .tile.done .dot{background:var(--gr);animation:none;opacity:1;transform:none}
  .body{
    padding:11px 13px;font:12.5px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;
    color:#c3cad9;white-space:pre-wrap;word-break:break-word;
    max-height:210px;overflow-y:auto;flex:1;
  }
  .tile.report .body{max-height:440px;font-size:13px;color:#d7dce8}
  .body::-webkit-scrollbar{width:8px}.body::-webkit-scrollbar-thumb{background:#2a3040;border-radius:8px}

  @keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.35;transform:scale(.66)}}
  @keyframes rise{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:none}}
  @keyframes pop{from{opacity:0;transform:scale(.98)}to{opacity:1;transform:none}}
  .empty{color:var(--mut);font-size:14px;padding:30px 0;text-align:center}
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>Resonate <span class="d">·</span> Research</h1>
    <div class="sub">Ask a question. A durable workflow plans searches, fans them out across Edge Function invocations, and streams every search and the final report back live — each on its own tile, all multiplexed on one channel.</div>
  </header>
  <form id="f">
    <input id="q" autocomplete="off" placeholder="What is durable execution?">
    <button type="submit">Research</button>
  </form>
  <div id="boards"><div class="empty">No runs yet — ask something above. Fire several; they stream side by side.</div></div>
</div>
<script>window.CFG={url:${JSON.stringify(SB_URL)},anon:${JSON.stringify(ANON)}};</script>
<script type="module">
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
const supabase = createClient(window.CFG.url, window.CFG.anon);
const boards = document.getElementById("boards");
const form = document.getElementById("f");
const input = document.getElementById("q");

form.addEventListener("submit", function (e) {
  e.preventDefault();
  const question = input.value.trim();
  if (!question) return;
  input.value = "";
  run(question);
});

function run(question) {
  const empty = boards.querySelector(".empty");
  if (empty) empty.remove();

  const id = "research-" + Math.random().toString(36).slice(2, 10);

  const board = document.createElement("div");
  board.className = "board";
  board.innerHTML =
    '<div class="bh"><div class="q"></div>' +
    '<div class="meta"><span class="chip"></span><span class="bs"></span></div></div>' +
    '<div class="tiles"></div>';
  board.querySelector(".q").textContent = question;
  board.querySelector(".chip").textContent = id;
  const bs = board.querySelector(".bs");
  bs.textContent = "planning…";
  boards.prepend(board);

  const tilesEl = board.querySelector(".tiles");
  const tiles = new Map();

  const ch = supabase.channel(id);
  ch.on("broadcast", { event: "stream" }, function (msg) {
    const p = msg.payload;
    if (p.kind === "bos") {
      bs.textContent = "streaming";
      const existing = tiles.get(p.id);
      if (existing) {
        // this stream re-ran (redispatch) — reset the tile, don't duplicate it
        existing.txt.textContent = "";
        existing.tile.classList.remove("done");
      } else {
        const isReport = p.label === "Report";
        const tile = document.createElement("div");
        tile.className = "tile" + (isReport ? " report" : "");
        tile.innerHTML =
          '<div class="th"><span class="dot"></span><span class="lbl"></span></div>' +
          '<div class="body"><span class="txt"></span></div>';
        tile.querySelector(".lbl").textContent = p.label;
        // sync every dot's pulse to one global phase so they breathe together
        tile.querySelector(".dot").style.animationDelay =
          "-" + (performance.now() % 1400) + "ms";
        tilesEl.appendChild(tile);
        tiles.set(p.id, {
          tile: tile,
          txt: tile.querySelector(".txt"),
          body: tile.querySelector(".body"),
          report: isReport,
        });
      }
    } else if (p.kind === "chunk") {
      const t = tiles.get(p.id);
      if (t) {
        const near = t.body.scrollHeight - t.body.scrollTop - t.body.clientHeight < 40;
        t.txt.textContent += p.data;
        if (near) t.body.scrollTop = t.body.scrollHeight;
      }
    } else if (p.kind === "eos") {
      const t = tiles.get(p.id);
      if (t) {
        t.tile.classList.add("done");
        if (t.report) { bs.textContent = "done"; bs.classList.add("done"); }
      }
    }
  });
  ch.subscribe(function (status) {
    if (status === "SUBSCRIBED") start(id, question, bs);
  });
}

async function start(id, question, bs) {
  try {
    const r = await fetch("/start", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ id: id, question: question }),
    });
    if (!r.ok) throw new Error(await r.text());
  } catch (e) {
    bs.textContent = "error";
    bs.classList.add("err");
  }
}
</script>
</body>
</html>`;
}
