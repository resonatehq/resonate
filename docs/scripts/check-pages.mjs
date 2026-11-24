import { readFileSync, realpathSync, statSync } from 'node:fs';
import { extname, join, relative, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { references } from './page-references.mjs';

const home = '/resonate/';
const quickstart = `${home}docs/get-started/quickstart/`;

export function checkPages(dist, origin = 'https://resonatehq.github.io') {
  const root = realpathSync(dist);
  const site = new URL(origin);
  const parsed = new Map();
  const checked = new Set();
  const errors = new Set();
  let homeLinksToQuickstart = false;

  function generatedFile(url) {
    if (!url.pathname.startsWith(home) && url.pathname !== home.slice(0, -1)) {
      throw Error(`escapes ${home}: ${url.pathname}`);
    }
    const segments = url.pathname.slice(home.length).split('/').map(decodeURIComponent);
    if (segments.some(s => s === '..' || /[/\\\0]/.test(s))) {
      throw Error(`invalid encoded path: ${url.pathname}`);
    }
    let file = join(root, ...segments);
    if (url.pathname.endsWith('/') || statSync(file, { throwIfNoEntry: false })?.isDirectory()) {
      // Pages redirects directory requests to the trailing-slash URL.
      if (!url.pathname.endsWith('/')) url.pathname += '/';
      file = join(file, 'index.html');
    }
    if (!statSync(file, { throwIfNoEntry: false })?.isFile()) {
      throw Error(`missing generated file: ${relative(root, file)}`);
    }
    file = realpathSync(file);
    if (!file.startsWith(root + sep)) throw Error('file is outside the build directory');
    return file;
  }

  function visit(value, base, source, kind) {
    try {
      const url = new URL(value, base);
      if (url.protocol === 'file:') throw Error('file URL cannot be served by Pages');
      if (!['http:', 'https:'].includes(url.protocol) || url.host !== site.host) return;
      const file = generatedFile(url);
      if (source === home && kind === 'a[href]' && file === join(root, quickstart.slice(home.length), 'index.html')) {
        homeLinksToQuickstart = true;
      }
      const ext = extname(file);
      if (!['.html', '.css', '.js', '.mjs'].includes(ext)) return;
      if (!parsed.has(file)) parsed.set(file, references(ext, readFileSync(file, 'utf8')));
      const page = parsed.get(file);
      if (ext === '.html' && url.hash) {
        const id = decodeURIComponent(url.hash.slice(1).split(':~:')[0]);
        if (id && id.toLowerCase() !== 'top' && !page.anchors.has(id)) {
          throw Error(`missing fragment #${id} in ${relative(root, file)}`);
        }
      }
      // Check fragments even if another link already visited the same page.
      if (checked.has(url.pathname)) return;
      checked.add(url.pathname);
      const refBase = new URL(page.base ?? '', url);
      if (page.base !== undefined && (refBase.origin !== url.origin || !(refBase.pathname + '/').startsWith(home))) {
        throw Error(`document base escapes ${home}: ${refBase.href}`);
      }
      for (const ref of page.refs) visit(ref.value, refBase, url.pathname, ref.kind);
    } catch (error) {
      errors.add(`${source} ${kind} ${JSON.stringify(value)}: ${error.message}`);
    }
  }

  for (const entry of [home, quickstart]) visit(entry, site, 'Pages', 'entry');
  if (!homeLinksToQuickstart) errors.add(`${home} has no working link to ${quickstart}`);
  if (errors.size) throw Error([...errors].join('\n'));
}

if (import.meta.main) {
  try {
    checkPages(fileURLToPath(new URL('../dist/', import.meta.url)), process.env.PAGES_ORIGIN || undefined);
    console.log(`Pages check passed: ${home}, ${quickstart}, and their local references.`);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
