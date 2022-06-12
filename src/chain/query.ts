import type { Config } from './base'
import ConditionChain from './condition'
import { camel } from 'froebel/string'
import type { Slice, CamelCase } from 'froebel/types'
import type { Schema, KeySym, ScFields, Projected, Field } from '../types'

type QueryConfig<T extends Schema<any>> = Config<T> & {
  key: any
  keyFilter?: KeyFilterArgs
}

type QueryResult<T> = {
  items: T[]
  lastKey?: unknown
  count: number
  scannedCount: number
  requestCount: number
}

export class Query<
  T extends Schema<any>,
  S extends string | number | symbol = Field<T>,
  SKF extends boolean = KeySym extends keyof T
    ? T[KeySym] extends any[]
      ? true
      : false
    : false
> extends ConditionChain<
  QueryResult<Projected<ScFields<T>, S>>,
  QueryConfig<T> & { verb: 'filter' },
  { limit: true; strong: true }
> {
  constructor(config: QueryConfig<T>) {
    super({ ...config, verb: 'filter' }, { limit: true, strong: true })
  }

  async execute() {
    this.resolve(await this.batchExec('query'))
  }

  [Symbol.asyncIterator] = this.batchIter<ScFields<T>>('query')

  public get expr(): AWS.DynamoDB.QueryInput {
    let KeyConditionExpression = `${this.name(this.pk)}=${this.value(
      this.config.key
    )}`
    if (this.config.keyFilter) {
      const [op, ...raw] = this.config.keyFilter
      const args = raw.map(v => this.value(v))
      if (!this.sk) throw Error("doesn't have sk")
      const key = this.name(this.sk)

      let cond =
        op.length <= 2
          ? `${key}${op}${args[0]}`
          : op === 'between'
          ? `${key} BETWEEN ${args.join(' AND ')}`
          : `${op}(${key}, ${args[0]})`

      KeyConditionExpression += ` AND ${cond}`
    }

    return this.createInput({
      KeyConditionExpression,
      ...(this.config.limit !== undefined && { Limit: this.config.limit }),
      ...Object.fromEntries(
        Object.entries({
          Limit: this.config.limit,
          FilterExpression: this.buildCondition()?.FilterExpression,
        }).filter(([_, v]) => v !== undefined)
      ),
    })
  }

  public where: SKF extends false
    ? never
    : KeyFilterBuilder<Query<T, S, false>> = ((f: any) =>
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

  public select<Fields extends string>(
    ...fields: Fields[]
  ): Query<T, Fields, SKF> {
    return this.clone({ selection: fields }) as any
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
) => R) & {
  [K in KeyFilterOp as CamelCase<K>]: (...args: Slice<KeyFilterArgs<K>, 1>) => R
}
