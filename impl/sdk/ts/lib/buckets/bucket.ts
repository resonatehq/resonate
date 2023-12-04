import { IBucket } from "../bucket";

export class Bucket implements IBucket {
  schedule<R>(thunk: () => R, delay: number = 0): Promise<R> {
    return new Promise((resolve, reject) => {
      setTimeout(async () => {
        try {
          resolve(await thunk());
        } catch (e) {
          reject(e);
        }
      }, delay);
    });
  }
}
