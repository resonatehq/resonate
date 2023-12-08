import {
  DurablePromise,
  PendingPromise,
  ResolvedPromise,
  RejectedPromise,
  CanceledPromise,
  TimedoutPromise,
} from "./promise";

/**
 * Promise Store API
 */
export interface IPromiseStore {
  /**
   * Creates a new durable promise
   *
   * @param id Unique identifier for the promise.
   * @param ikey Idempotency key associated with the create operation.
   * @param strict If true, deduplicates only if the promise is pending.
   * @param headers Key value pairs associated with the data.
   * @param data Encoded data of type string.
   * @param timeout Time (in milliseconds) after which the promise is considered expired.
   * @param tags Key value pairs associated with the promise.
   * @returns A durable promise that is pending, canceled, resolved, or rejected.
   */
  create(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
    timeout: number,
    tags: Record<string, string> | undefined,
  ): Promise<PendingPromise | CanceledPromise | ResolvedPromise | RejectedPromise | TimedoutPromise>;

  /**
   * Cancels a new promise.
   *
   * @param id Unique identifier for the promise.
   * @param ikey Idempotency key associated with the create operation.
   * @param strict If true, deduplicates only if the promise is canceled.
   * @param headers Key value pairs associated with the data.
   * @param data Encoded data of type string.
   * @returns A durable promise that is canceled, resolved, or rejected.
   */
  cancel(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<CanceledPromise | ResolvedPromise | RejectedPromise | TimedoutPromise>;

  /**
   * Resolves a promise.
   *
   * @param id Unique identifier for the promise to be resolved.
   * @param ikey Idempotency key associated with the resolve promise.
   * @param strict If true, deduplicates only if the promise is resolved.
   * @param headers Key value pairs associated with the data.
   * @param data Encoded data of type string.
   * @returns A durable promise that is canceled, resolved, or rejected.
   */
  resolve(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<CanceledPromise | ResolvedPromise | RejectedPromise | TimedoutPromise>;

  /**
   * Rejects a promise
   *
   * @param id Unique identifier for the promise to be rejected.
   * @param ikey Integration key associated with the promise.
   * @param strict If true, deduplicates only if the promise is rejected.
   * @param headers Key value pairs associated with the data.
   * @param data Encoded data of type string.
   * @returns A durable promise that is canceled, resolved, or rejected.
   */
  reject(
    id: string,
    ikey: string | undefined,
    strict: boolean,
    headers: Record<string, string> | undefined,
    data: string | undefined,
  ): Promise<CanceledPromise | ResolvedPromise | RejectedPromise | TimedoutPromise>;

  /**
   * Retrieves a promise based on its id.
   *
   * @param id Unique identifier for the promise to be retrieved.
   * @returns A durable promise that is pending, canceled, resolved, or rejected.
   */
  get(id: string): Promise<DurablePromise>;
}
