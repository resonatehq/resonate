import * as css from 'css-tree';
import { parse as parseModule } from 'es-module-lexer';
import parseSrcset from 'parse-srcset';
import { parse as parseHtml } from 'parse5';

// Read generated markup, not source MDX: the generator supplies heading IDs,
// prefixes, responsive image candidates and hashed bundle names.
export function references(extension, source) {
  const refs = [];
  const anchors = new Set();
  const add = (value, kind) => refs.push({ value, kind });
  let base;

  function stylesheet(text, context) {
    css.walk(css.parse(text, { context, parseCustomProperty: true }), {
      enter(node) {
        if (node.type === 'Url' && !node.value.startsWith('#')) add(node.value, 'css url');
        if (node.type === 'Atrule' && node.name.toLowerCase() === 'import') {
          const first = node.prelude?.children.first;
          if (first?.type === 'String') add(first.value, '@import');
        }
      },
    });
  }

  function script(text) {
    for (const entry of parseModule(text)[0]) {
      // Computed runtime URLs and bare import-map names are not file URLs.
      if (entry.specifier && !entry.glob && /^(?:[./]|https?:)/i.test(entry.specifier)) {
        add(entry.specifier, 'module import');
      }
    }
  }

  function element(node) {
    const tag = node.tagName;
    const attrs = Object.fromEntries((node.attrs ?? []).map(({ name, value }) => [name, value]));
    if (attrs.id) anchors.add(attrs.id);
    if (tag === 'a' && attrs.name) anchors.add(attrs.name);
    if (tag === 'base') {
      base ??= attrs.href;
    } else {
      for (const attr of ['href', 'src', 'poster', ...(tag === 'object' ? ['data'] : [])]) {
        if (attr in attrs) add(attrs[attr], `${tag}[${attr}]`);
      }
    }
    for (const attr of ['srcset', 'imagesrcset']) {
      if (attrs[attr]) for (const { url } of parseSrcset(attrs[attr])) add(url, `${tag}[${attr}]`);
    }
    if (attrs.style) stylesheet(attrs.style, 'declarationList');
    const text = node.childNodes?.[0]?.value;
    if (text && tag === 'style') stylesheet(text, 'stylesheet');
    if (text && tag === 'script' && attrs.type === 'module') script(text);
    node.childNodes?.forEach(element);
  }

  if (extension === '.html') element(parseHtml(source));
  else if (extension === '.css') stylesheet(source, 'stylesheet');
  else script(source);
  return { anchors, base, refs };
}
