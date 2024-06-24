import { Options, isOptions } from "./options";

export function randomId(): string {
  return Math.floor(Math.random() * Number.MAX_SAFE_INTEGER).toString(16);
}

export function hash(s: string): string {
  let h = 0;
  for (let i = 0; i < s.length; i++) {
    h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  }

  // Generate fixed length hexadecimal hash
  const hashString = (Math.abs(h) >>> 0).toString(16); // Convert to unsigned int and then to hexadecimal
  const maxLength = 8;
  return "0".repeat(Math.max(0, maxLength - hashString.length)) + hashString;
}

export function split(argsWithOpts: any[]): { args: any[]; opts: Partial<Options> } {
  const possibleOpts = argsWithOpts.at(-1);
  return isOptions(possibleOpts)
    ? { args: argsWithOpts.slice(0, -1), opts: possibleOpts }
    : { args: argsWithOpts, opts: {} };
}
