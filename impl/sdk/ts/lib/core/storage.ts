export interface IStorage<T> {
  rmw<X extends T | undefined>(id: string, func: (item: T | undefined) => X): Promise<X>;
  rmd(id: string, func: (item: T) => boolean): Promise<boolean>;
  all(): AsyncGenerator<T[], void>;
}
