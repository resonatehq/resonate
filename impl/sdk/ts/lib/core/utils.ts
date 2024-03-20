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
