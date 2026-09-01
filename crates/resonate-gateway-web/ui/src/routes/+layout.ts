/**
 * A single-page app: no server, no prerender.
 *
 * The console reads a running server through the protocol, so there is nothing
 * to render ahead of time and no build-time knowledge of what exists. Every
 * route resolves to the same document and the client takes it from there.
 */
export const ssr = false;
export const prerender = false;
export const trailingSlash = 'never';
