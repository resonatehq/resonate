import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { checkPages } from './check-pages.mjs';

const guide = 'docs/get-started/quickstart';
const origin = 'https://project.example';
const landing = `<a href="./${guide}?from=home#install%20SDK">Quickstart</a>`;
const quickstart = `
  <h2 id="install SDK">Install</h2><a name="legacy"></a>
  <a href="#legacy">Legacy anchor</a><a href="../../../#top">Home</a>
  <a href="//project.example/resonate/api/ts/#method">API</a>
  <img srcset="data:image/png;base64,AAAA 1x, ../../../img/echo.png?v=1 2x">
  <link rel=stylesheet href="/resonate/css/site.css">
  <script type=module src="/resonate/js/main.js"></script>
  <a href="https://other.example/missing#external">External</a>
  <a href="mailto:docs@example.org">Email</a>`;

function fixture(t, changes = {}) {
  const root = mkdtempSync(join(tmpdir(), 'resonate-pages-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const files = {
    'index.html': landing,
    [`${guide}/index.html`]: quickstart,
    'api/ts/index.html': '<h2 id=method>API</h2>',
    'img/echo.png': '',
    'css/site.css': '@import "theme.css";',
    'css/theme.css': '.logo { background-image: url(../img/echo.png) }',
    'js/main.js': 'import "./chunk.js";',
    'js/chunk.js': '',
    ...changes,
  };
  for (const [name, contents] of Object.entries(files)) {
    if (contents === null) continue;
    const file = join(root, name);
    mkdirSync(dirname(file), { recursive: true });
    writeFileSync(file, contents);
  }
  return () => checkPages(root, origin);
}

test('resolves both entry points and their local references offline', t => {
  fixture(t)();
});

for (const [label, changes, failure] of [
  ['missing landing page', { 'index.html': null }, /Pages entry .*missing generated file: index.html/],
  ['missing quickstart', { [`${guide}/index.html`]: null }, /Pages entry .*missing generated file: docs\/get-started\/quickstart\/index.html/],
  ['unlinked quickstart', { 'index.html': '<a href="/">Elsewhere</a>' }, /no working link to/],
  ['root-relative asset', { 'index.html': `${landing}<img src="/img/echo.png">` }, /img\[src\].*escapes \/resonate\//],
  ['relative escape', { [`${guide}/index.html`]: '<a href="../../../../other/">Bad</a>' }, /escapes \/resonate\/: \/other\//],
  ['lookalike absolute prefix', { 'index.html': `${landing}<a href="${origin}/resonate-other/">Bad</a>` }, /escapes \/resonate\//],
  ['document base outside project', { 'index.html': `<base href="/other/">${landing}` }, /document base escapes/],
  ['cross-page fragment', { 'api/ts/index.html': '<h2 id=renamed>API</h2>' }, /missing fragment #method in api\/ts\/index.html/],
  ['same-page fragment after revisit', { [`${guide}/index.html`]: quickstart + '<a href="#gone">Bad</a>' }, /missing fragment #gone/],
  ['missing responsive image', { 'img/echo.png': null }, /img\[srcset\].*missing generated file: img\/echo.png/],
  ['missing stylesheet import', { 'css/theme.css': null }, /@import.*missing generated file: css\/theme.css/],
  ['missing font in a stylesheet', { 'css/theme.css': '@font-face{src:url(../fonts/font.woff2)}' }, /css url.*missing generated file: fonts\/font.woff2/],
  ['missing imported module', { 'js/chunk.js': null }, /module import.*missing generated file: js\/chunk.js/],
]) {
  test(`rejects ${label}`, t => assert.throws(fixture(t, changes), failure));
}
