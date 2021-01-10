export default abstract class BaseChain<T> extends Promise<T> {
  static get [Symbol.species]() {
    return Promise
  }

  // @ts-ignore
  constructor(protected readonly client: AWS.DynamoDB.DocumentClient) {
    let _resolve: any
    let _reject: any
    super((res, rej) => {
      _resolve = res
      _reject = rej
    })
    this.resolve = _resolve!
    this.reject = _reject!
  }

  protected resolve!: (v: T | PromiseLike<T>) => void
  protected reject!: (reason?: any) => void

  public then<TResult1 = T, TResult2 = never>(
    res?: (value: T) => TResult1 | PromiseLike<TResult1>,
    rej?: (reason: any) => TResult2 | PromiseLike<TResult2>
  ): Promise<TResult1 | TResult2> {
    this.execute()
    return super.then(res, rej)
  }

  protected abstract execute(): Promise<void>
}
