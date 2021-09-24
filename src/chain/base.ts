import type { Schema, ExplTypes, KeySym, ScFields } from '../types'
import { clone } from '../utils/object'
import * as naming from '../utils/naming'
import { decode } from '../utils/convert'
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
}

export type UtilFlags = { cast?: boolean; limit?: boolean }

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

  protected async batchExec<T extends 'scan' | 'query'>(
    op: T,
    params: Parameters<AWS.DynamoDB.DocumentClient[T]>[0]
  ) {
    const result = {
      items: Array<any>(),
      count: 0,
      scannedCount: 0,
      lastKey: null as unknown,
      requests: 0,
    }

    do {
      this.log(`[batch] ${op}`, params)
      const res = await this.config.client[op](params).promise()

      result.items.push(...(res.Items ?? []))
      result.lastKey = res.LastEvaluatedKey
      result.count += res.Count!
      result.scannedCount += res.ScannedCount!
      result.requests += 1

      params.ExclusiveStartKey = res.LastEvaluatedKey
      if (params.Limit) params.Limit -= res.Items?.length ?? 0
    } while (
      params.ExclusiveStartKey &&
      (params.Limit ?? Infinity) > 0 &&
      result.requests < (this.config.maxRequests ?? Infinity)
    )

    result.items = result.items.map(decode)
    if (this.config.selection)
      result.items = result.items.map(v => pick(v, ...this.config.selection!))
    return result
  }
}
