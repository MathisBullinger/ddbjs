import type { Schema, ExplTypes, KeySym, ScFields } from '../types'
import * as naming from '../utils/naming'
import { decode } from '../utils/convert'
import clone from 'snatchblock/clone'
import BiMap from 'snatchblock/bimap'
import callAll from 'snatchblock/callAll'
import pick from 'snatchblock/pick'

export type Config<T extends Schema<any>> = {
  schema: T
  client: AWS.DynamoDB.DocumentClient
  table: string
  strong?: boolean
  debug?: boolean
  limit?: number
  selection?: any[]
  maxRequests?: number
  batchSize?: number
  from?: any
}

export type UtilFlags = { cast?: boolean; limit?: boolean; strong?: boolean }

export default abstract class BaseChain<
  TResult,
  TConfig extends Config<any>,
  TUtil extends UtilFlags = {}
> extends Promise<TResult> {
  static get [Symbol.species]() {
    return Promise
  }

  static key?: KeySym

  // @ts-ignore
  constructor(
    protected readonly config: TConfig,
    private readonly utils: TUtil
  ) {
    let _resolve: any
    let _reject: any
    super((res, rej) => {
      _resolve = res
      _reject = rej
    })
    this.resolve = _resolve!
    this.reject = _reject!

    for (const [flag, key] of this.flagged)
      if (!utils[flag]) delete (this as any)[key]

    this.onCloneHooks.push(chain => {
      this.copyState(chain)
    })
  }

  protected resolve!: (v: TResult | PromiseLike<TResult>) => void
  protected reject!: (reason?: any) => void

  public then<TResult1 = TResult, TResult2 = never>(
    res?: (value: TResult) => TResult1 | PromiseLike<TResult1>,
    rej?: (reason: any) => TResult2 | PromiseLike<TResult2>
  ): Promise<TResult1 | TResult2> {
    this.execute().catch(this.reject)
    return super.then(res, rej)
  }

  protected abstract execute(): Promise<void>
  public abstract get expr(): any

  public debug(): this {
    return this.clone({ debug: true } as Partial<TConfig>)
  }

  protected log(method: string, params?: Record<string, any>) {
    if (this.config.debug) console.log(method, params ?? '')
  }

  protected attrNames = BiMap.alias('key', 'name')<string, string>()
  protected attrValues = BiMap.alias('key', 'value')<string, any>()

  protected name(path: string): string {
    return naming.join(
      ...naming
        .parts(path)
        .map(v =>
          v.startsWith('[') || naming.valid(v)
            ? v
            : (this.attrNames.name[v] ??= `#n${this.attrNames.size}`)
        )
    )
  }

  protected value(val: unknown) {
    return this.attrValues.value.getOrSet(val, `:v${this.attrValues.size}`)
  }

  protected copyState(chain: BaseChain<any, any, any>) {
    chain.attrNames = this.attrNames.clone()
    chain.attrValues = this.attrValues.clone()
  }

  protected createInput<T extends Record<string, any>>(
    input?: T
  ): T & {
    TableName: string
    ExpressionAttributeNames?: Record<string, string>
    ExpressionAttributeValues?: Record<string, any>
  } {
    return {
      ...input,
      TableName: this.config.table,
      ...(this.attrNames.size && {
        ExpressionAttributeNames: Object.fromEntries(this.attrNames.key),
      }),
      ...(this.attrValues.size && {
        ExpressionAttributeValues: Object.fromEntries(this.attrValues.key),
      }),
      ...(this.config.strong && { ConsistentRead: true }),
    } as any
  }

  protected isSet(key: string, entry = this.config.schema): boolean {
    const type = entry[key]
    if (!Array.isArray(type)) return false
    return type.length === 1
  }

  protected makeSets(target: any, entry: any = this.config.schema) {
    if (!target) return
    const mapped = { ...target } as any
    for (const [k, v] of Object.entries(mapped)) {
      if (
        k in this.config.schema &&
        typeof v === 'object' &&
        !Array.isArray(v) &&
        v !== null
      )
        mapped[k] = this.makeSets(v, this.config.schema[k])
      else if (this.isSet(k, entry) && Array.isArray(v))
        mapped[k] = this.createSet(v)
    }
    return mapped
  }

  protected createSet(v: any[]) {
    return this.config.client.createSet(v)
  }

  protected onCloneHooks: ((chain: this) => void)[] = []

  private flagged: [keyof TUtil, string][] = []
  private flag<T, F extends keyof TUtil>(
    flag: F,
    fun: T,
    key = flag as string
  ): TUtil[F] extends true ? T : never {
    this.flagged.push([flag, key])
    return fun as any
  }

  public cast = this.flag(
    'cast',
    (casts: ExplTypes<ScFields<TConfig['schema']>>) => {
      const schema = clone(this.config.schema)

      const apply = (
        type: 'Set' | 'List',
        path: string[],
        target: any = schema
      ) => {
        if (path.length > 1)
          apply(type, path.slice(1), (target[path[0]] ??= {}))
        else target[path[0]] = type === 'List' ? [] : [String]
      }

      for (const [path, type] of Object.entries(casts))
        apply(type, path.split('.'))

      return this.clone({ schema } as Partial<TConfig>)
    }
  )

  public limit = this.flag('limit', (limit: number) =>
    this.clone({ limit } as Partial<TConfig>)
  )

  public maxRequests = this.flag(
    'limit',
    (max: number) => this.clone({ maxRequests: max } as Partial<TConfig>),
    'maxRequests'
  )

  public batchSize = this.flag(
    'limit',
    (size: number) => this.clone({ batchSize: size } as Partial<TConfig>),
    'batchSize'
  )

  public from = this.flag(
    'limit',
    (key: any) => this.clone({ from: key } as Partial<TConfig>),
    'from'
  )

  public strong = this.flag('strong', () =>
    this.clone({ strong: true } as Partial<TConfig>)
  )

  protected clone(diff: Partial<TConfig> = {}): this {
    const copy = new (this as any).constructor(
      { ...this.config, ...diff },
      this.utils
    )
    callAll(this.onCloneHooks, copy)
    return copy
  }

  protected get keyFields(): string[] {
    return typeof this.config.schema[BaseChain.key!] === 'string'
      ? [this.config.schema[BaseChain.key!] as string]
      : (this.config.schema[BaseChain.key!] as string[])
  }

  protected get pk(): string {
    const key = (this.config.schema as any)[BaseChain.key!]
    return Array.isArray(key) ? key[0] : (key as any)
  }

  protected get sk(): string | null {
    const key = (this.config.schema as any)[BaseChain.key!]
    return key[1] ?? null
  }

  protected async batchExec<T extends 'scan' | 'query'>(op: T) {
    const result = {
      items: Array<any>(),
      count: 0,
      scannedCount: 0,
      lastKey: null as unknown,
      requestCount: 0,
    }

    for await (const item of this.batchIter(op)(v => {
      result.lastKey = v.LastEvaluatedKey
      result.count += v.Count ?? 0
      result.scannedCount += v.ScannedCount ?? 0
      result.requestCount += 1
    }))
      result.items.push(item)

    return result
  }

  protected batchIter = <T = any>(
    op: 'scan' | 'query',
    getParams: () => Parameters<AWS.DynamoDB.DocumentClient['query']>[0] = () =>
      this.expr,
    config: Config<any> = this.config
  ) =>
    async function* (
      onBatch?: (res: AWS.DynamoDB.DocumentClient.QueryOutput) => void
    ): AsyncGenerator<T> {
      const params = getParams()
      if (config.from) params.ExclusiveStartKey = config.from
      let itemCount = 0
      let requestCount = 0

      do {
        if (config.limit) params.Limit = config.limit - itemCount
        if (config.batchSize! < (params.Limit ?? Infinity))
          params.Limit = config.batchSize

        const res = await config.client[op](params).promise()
        onBatch?.(res)
        params.ExclusiveStartKey = res.LastEvaluatedKey
        itemCount += res.Items?.length ?? 0

        for (const item of res.Items ?? []) {
          let val: any = decode(item)
          if (config.selection) val = pick(val, ...config.selection!)
          yield val
        }
      } while (
        params.ExclusiveStartKey &&
        itemCount < (config.limit ?? Infinity) &&
        ++requestCount < (config.maxRequests ?? Infinity)
      )
    }
}
