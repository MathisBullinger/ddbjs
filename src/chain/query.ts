import BaseChain, { Config } from './base'
import { decode } from '../utils/convert'
import { camel } from 'snatchblock/string'
import type { Slice, CamelCase } from 'snatchblock/types'
import type { Schema, KeySym, ScItem } from '../types'

type QueryConfig<T extends Schema<any>> = Config<T> & {
  key: any
  keyFilter?: KeyFilterArgs
}

export class Query<
  T extends Schema<any>,
  SKF extends boolean = KeySym extends keyof T
    ? T[KeySym] extends any[]
      ? true
      : false
    : false
> extends BaseChain<ScItem<T>[], QueryConfig<T>> {
  constructor(config: QueryConfig<T>) {
    super(config, {})
  }

  async execute() {
    let KeyConditionExpression = `${this.name(this.pk)}=${this.value(
      this.config.key
    )}`
    if (this.config.keyFilter) {
      const [op, ...raw] = this.config.keyFilter
      const args = raw.map(v => this.value(v))
      const key = this.name(this.sk)

      let cond =
        op.length <= 2
          ? `${key}${op}${args[0]}`
          : op === 'between'
          ? `${key} BETWEEN ${args.join(' AND ')}`
          : `${op}(${key}, ${args[0]})`

      KeyConditionExpression += ` AND ${cond}`
    }

    const params = this.createInput({ KeyConditionExpression })
    super.log('query', params)

    const res = await this.config.client.query(params).promise()
    this.resolve(res.Items?.map(decode) as any)
  }

  public where: SKF extends false ? never : KeyFilterBuilder<Query<T, false>> =
    ((f: any) =>
      Object.assign(
        f,

        Object.fromEntries(
          filterOps.map(op => [camel(op), (...args: any[]) => f(op, ...args)])
        )
      ))((...args: KeyFilterArgs) =>
      this.clone({
        keyFilter: [args[0].toLowerCase(), ...args.slice(1)] as KeyFilterArgs,
      })
    )

  private get pk(): string {
    const key = (this.config.schema as any)[BaseChain.key!]
    return Array.isArray(key) ? key[0] : (key as any)
  }

  private get sk(): string {
    const key = (this.config.schema as any)[BaseChain.key!]
    if (!Array.isArray(key) || key.length < 2) throw Error("doesn't have sk")
    return key[1] as string
  }
}

const filterOps = ['=', '<', '<=', '>', '>=', 'between', 'begins_with'] as const

type KeyFilterOp = typeof filterOps[number]

type KeyFilterArgs<T extends KeyFilterOp = any, K = unknown> = [
  op: T,
  ...args: T extends 'between' ? [a: K, b: K] : [comp: K]
]

type KeyFilterBuilder<R> = (<T extends KeyFilterOp, K>(
  ...args: KeyFilterArgs<T, K>
) => R) &
  {
    [K in KeyFilterOp as CamelCase<K>]: (
      ...args: Slice<KeyFilterArgs<K>, 1>
    ) => R
  }
