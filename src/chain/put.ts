import ConditionChain from './condition'
import { decode } from '../utils/convert'
import { assert, ReturnValueError } from '../utils/error'
import { oneOf } from '../utils/array'
import * as expr from '../expression'
import type { Fields, DBItem } from '../types'

type ReturnType = 'NONE' | 'NEW' | 'OLD'

export class PutChain<
  T extends Fields,
  R extends ReturnType,
  F = R extends 'NONE' ? undefined : DBItem<T>
> extends ConditionChain<F, T> {
  constructor(
    fields: T,
    client: AWS.DynamoDB.DocumentClient,
    private readonly keyFields: string[],
    private readonly params: AWS.DynamoDB.DocumentClient.PutItemInput,
    private readonly returnType: ReturnType = 'NONE',
    debug?: boolean
  ) {
    super(fields, client, debug)
  }

  async execute() {
    this.params.Item = this.makeSets(this.params.Item)

    const params = {
      ...this.params,
      ...(this.returnType === 'OLD' && {
        ReturnValues: 'ALL_OLD',
      }),
    }
    Object.assign(params, expr.merge(params as any, this.buildCondition()))
    super.log('put', params)

    const { Attributes } = await this.client.put(params).promise()

    const result: F = decode(
      this.returnType === 'NEW'
        ? this.params.Item
        : this.returnType === 'OLD'
        ? Attributes
        : undefined
    ) as any

    this.resolve(result)
  }

  returning<R extends ReturnType>(v: R): PutChain<T, R> {
    assert(oneOf(v, 'NEW', 'OLD', 'NONE'), new ReturnValueError(v, 'insert'))
    return this.clone(this.fields, this._debug, v)
  }

  public ifNotExists() {
    let chain = this
    for (const k of this.keyFields)
      chain = chain.if(k, '<>', this.params.Item[k]) as any
    return chain
  }

  public cast = super._cast.bind(this)

  protected clone(
    fields = this.fields,
    debug = this._debug,
    returnType = this.returnType
  ) {
    const chain = new PutChain(
      fields,
      this.client,
      this.keyFields,
      this.params,
      returnType,
      debug
    ) as any
    chain.conditions = this.cloneConditon()
    chain.names = { ...this.names }
    chain.values = { ...this.values }
    return chain
  }
}
