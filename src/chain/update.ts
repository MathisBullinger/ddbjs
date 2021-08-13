import BaseChain from './base'
import ConditionChain from './condition'
import * as expr from '../expression'
import { decode } from '../utils/convert'
import type { Fields, Schema, DBItem, UpdateInput, KeySym } from '../types'
import { mapValues } from '../utils/object'

type ReturnType = 'NONE' | 'OLD' | 'NEW' | 'UPDATED_OLD' | 'UPDATED_NEW'

type UpdateOpts = {
  table: string
  key: any
}

export class UpdateChain<
  T extends Schema<F>,
  RT extends ReturnType,
  F extends Fields = Omit<T, KeySym>,
  RV = RT extends 'NONE'
    ? undefined
    : RT extends 'OLD' | 'NEW'
    ? DBItem<F>
    : Partial<DBItem<F>>
> extends ConditionChain<RV, F> {
  constructor(
    fields: F,
    client: AWS.DynamoDB.DocumentClient,
    private readonly update: UpdateInput<T, F> & UpdateOpts,
    private readonly returnType: ReturnType = 'NONE',
    debug?: boolean
  ) {
    super(fields, client, debug)
  }

  async execute() {
    const params: Partial<AWS.DynamoDB.DocumentClient.UpdateItemInput> = {
      TableName: this.update.table,
      Key: this.update.key,
    }

    this.update.set = this.makeSets(this.update.set)

    Object.assign(
      params,
      expr.merge(
        expr.set(this.update.set as any),
        expr.remove(...(this.update.remove ?? [])),
        expr.add(
          this.update.add &&
            mapValues(this.update.add, v =>
              Array.isArray(v) ? this.client.createSet(v) : v
            )
        ),
        expr.del(
          this.update.delete &&
            mapValues(this.update.delete, v => this.client.createSet(v))
        )
      )
    )

    params.ReturnValues = ['NEW', 'OLD'].includes(this.returnType)
      ? `ALL_${this.returnType}`
      : this.returnType

    Object.assign(params, expr.merge(params as any, this.buildCondition()))

    if (!this.isComplete(params)) throw Error('incomplete update')

    super.log('update', params)
    const { Attributes } = await this.client.update(params).promise()
    const result: RV = decode(
      this.returnType === 'NONE' ? undefined : Attributes
    ) as any
    this.resolve(result)
  }

  remove(...fields: string[]): UpdateChain<T, RT, F, RV> {
    const update = this.update
    update.remove = [...(update.remove ?? []), ...fields]
    return this.clone(this.fields, this._debug, update)
  }

  add(
    fields: Exclude<UpdateInput<T, F>['add'], null>
  ): UpdateChain<T, RT, F, RV> {
    const update = this.update
    update.add = { ...update.add, ...fields }
    return this.clone(this.fields, this._debug, update)
  }

  delete(
    fields: Exclude<UpdateInput<T, F>['delete'], null>
  ): UpdateChain<T, RT, F, RV> {
    const update = this.update
    update.delete = { ...update.delete, ...fields }
    return this.clone(this.fields, this._debug, update)
  }

  returning<R extends ReturnType>(v: R): UpdateChain<T, R, F> {
    return this.clone(this.fields, this._debug, this.update, v)
  }

  ifExists() {
    let chain = this
    for (const [k, v] of Object.entries(this.update.key))
      chain = chain.if(k, '=', { literal: v as any }) as any
    return chain
  }

  private isComplete(
    input: Partial<AWS.DynamoDB.DocumentClient.UpdateItemInput>
  ): input is AWS.DynamoDB.DocumentClient.UpdateItemInput {
    if (!input.UpdateExpression?.length)
      throw Error('missing update expression')
    return true
  }

  public cast = super._cast.bind(this)

  protected clone(
    fields = this.fields,
    debug = this._debug,
    update = this.update,
    returnType = this.returnType
  ) {
    const chain = new (UpdateChain as any)(
      fields,
      this.client,
      update,
      returnType,
      debug
    ) as any
    chain.condition = this.cloneConditon()
    chain.names = { ...this.names }
    chain.values = { ...this.values }
    return chain
  }
}
