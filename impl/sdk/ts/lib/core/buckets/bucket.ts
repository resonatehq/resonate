import { IBucket } from "../bucket";

export class Bucket implements IBucket {
  async schedule<R>(thunk: () => R, delay: number = 0): Promise<R> {
    await new Promise((resolve) => setTimeout(resolve, delay));
    return await thunk();
  }
}
