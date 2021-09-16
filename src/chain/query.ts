import BaseChain from './base'
import { decode } from '../utils/convert'
import omit from 'snatchblock/omit'
import type { Schema, Fields, DBItem, KeySym } from '../types'

export class Query<
  T extends Schema<F>,
  F extends Fields,
  SKF extends boolean = T[KeySym] extends any[] ? true : false
> extends BaseChain<DBItem<T>[], T> {
  constructor(
    private readonly schema: T,
    client: AWS.DynamoDB.DocumentClient,
    table: string,
    private readonly keyValue: any,
    private readonly keyFilter?: KeyFilterArgs,
    debug?: boolean
  ) {
    super(omit(schema, BaseChain.key!) as any, client, table, debug)
  }

  async execute() {
    let KeyConditionExpression = `${this.name(this.pk)}=${this.value(
      this.keyValue
    )}`
    if (this.keyFilter) {
      const [op, ...raw] = this.keyFilter
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

    const res = await this.client.query(params).promise()
    this.resolve(res.Items?.map(decode) as any)
  }

  public where: SKF extends false
    ? never
    : KeyFilterBuilder<Query<T, F, false>> = ((...args: KeyFilterArgs) =>
    this.clone(undefined, undefined, [
      args[0].toLowerCase(),
      ...args.slice(1),
    ] as KeyFilterArgs)) as any

  private get pk(): string {
    const key = this.schema[BaseChain.key!]
    return Array.isArray(key) ? key[0] : (key as any)
  }

  private get sk(): string {
    const key = this.schema[BaseChain.key!]
    if (!Array.isArray(key) || key.length < 2) throw Error("doesn't have sk")
    return key[1] as string
  }

  protected clone(
    _?: Fields,
    debug?: boolean,
    keyFilter?: KeyFilterArgs
  ): this {
    return new Query(
      this.schema as any,
      this.client,
      this.table,
      this.keyValue,
      keyFilter ?? this.keyFilter,
      debug ?? this._debug
    ) as any
  }
}

type KeyFilterOp = '=' | '<' | '<=' | '>' | '>=' | 'between' | 'begins_with'

type KeyFilterArgs<T extends KeyFilterOp = any, K = unknown> = [
  op: T,
  ...args: T extends 'between' ? [a: K, b: K] : [comp: K]
]

type KeyFilterBuilder<R> = <T extends KeyFilterOp, K>(
  ...args: KeyFilterArgs<T, K>
) => R
