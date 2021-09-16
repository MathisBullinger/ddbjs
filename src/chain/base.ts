import type { Fields, ExplTypes, KeySym } from '../types'
import BiMap from 'snatchblock/bimap'
import { clone } from '../utils/object'
import * as naming from '../utils/naming'

export default abstract class BaseChain<
  T,
  F extends Fields
> extends Promise<T> {
  static get [Symbol.species]() {
    return Promise
  }

  static key?: KeySym

  // @ts-ignore
  constructor(
    readonly fields: F,
    protected readonly client: AWS.DynamoDB.DocumentClient,
    protected readonly table: string,
    protected _debug = false
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

  public debug(): this {
    return this.clone(this.fields, true)
  }

  protected log(method: string, params?: Record<string, any>) {
    if (this._debug) console.log(method, params ?? '')
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

  protected copyState(chain: BaseChain<any, any>) {
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
      TableName: this.table,
      ...(this.attrNames.size && {
        ExpressionAttributeNames: Object.fromEntries(this.attrNames.key),
      }),
      ...(this.attrValues.size && {
        ExpressionAttributeValues: Object.fromEntries(this.attrValues.key),
      }),
    } as any
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

  protected _cast(casts: ExplTypes<F>): this {
    const fields = clone(this.fields)

    const apply = (
      type: 'Set' | 'List',
      path: string[],
      target: any = fields
    ) => {
      if (path.length === 1) {
        target[path[0]] = type === 'List' ? [] : [String]
        return
      }
      apply(type, path.slice(1), (target[path[0]] ??= {}))
    }

    for (const [path, type] of Object.entries(casts))
      apply(type, path.split('.'))

    return this.clone(fields, this._debug)
  }

  protected abstract clone(fields?: F, debug?: boolean): this
}
