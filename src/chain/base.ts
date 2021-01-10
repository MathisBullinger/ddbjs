export default abstract class BaseChain<
  T,
  F extends Fields
> extends Promise<T> {
  static get [Symbol.species]() {
    return Promise
  }

  // @ts-ignore
  constructor(
    readonly fields: F,
    protected readonly client: AWS.DynamoDB.DocumentClient
  ) {
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
    this.execute().catch(this.reject)
    return super.then(res, rej)
  }

  protected abstract execute(): Promise<void>

  protected static encodeKey(v: string) {
    return Buffer.from(v).toString('hex')
  }

  protected static decodeKey(v: string) {
    return Buffer.from(v, 'hex').toString()
  }

  protected isSet(key: string, entry = this.fields): boolean {
    const type = entry[key]
    if (!Array.isArray(type)) return false
    return type.length === 1
  }

  protected makeSets(target: any, entry: any = this.fields) {
    if (!target) return
    const mapped = { ...target } as any
    for (const [k, v] of Object.entries(mapped)) {
      if (
        k in this.fields &&
        typeof v === 'object' &&
        !Array.isArray(v) &&
        v !== null
      )
        mapped[k] = this.makeSets(v, this.fields[k])
      else if (this.isSet(k, entry) && Array.isArray(v))
        mapped[k] = this.createSet(v)
    }
    return mapped
  }

  protected createSet(v: any[]) {
    return this.client.createSet(v)
  }
}
