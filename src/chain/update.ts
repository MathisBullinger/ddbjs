import type { Config } from './base'
import ConditionChain from './condition'
import * as expr from '../expression'
import { decode } from '../utils/convert'
import type { Schema, UpdateInput, ScItem } from '../types'
import { mapValues, clone } from '../utils/object'

type ReturnType = 'NONE' | 'OLD' | 'NEW' | 'UPDATED_OLD' | 'UPDATED_NEW'

type UpdateConfig<T extends Schema<any>, R extends ReturnType> = Config<T> & {
  return: R
  key: any
  update: Input<T>
}
type Input<T extends Schema<any>> = T extends Schema<infer F>
  ? UpdateInput<T, F>
  : never

export class Update<
  T extends Schema<any>,
  R extends ReturnType
> extends ConditionChain<
  R extends 'NONE'
    ? undefined
    : R extends 'OLD' | 'NEW'
    ? ScItem<T>
    : Partial<ScItem<T>>,
  UpdateConfig<T, R>,
  { cast: true }
> {
  constructor(config: UpdateConfig<T, R>) {
    super(config, { cast: true })
  }

  async execute() {
    const params: Partial<AWS.DynamoDB.DocumentClient.UpdateItemInput> = {
      TableName: this.config.table,
      Key: this.config.key,
    }

    this.config.update.set = this.makeSets(this.config.update.set)

    Object.assign(
      params,
      expr.merge(
        expr.set(this.config.update.set as any),
        expr.remove(...(this.config.update.remove ?? [])),
        expr.add(
          this.config.update.add &&
            mapValues(this.config.update.add, v =>
              Array.isArray(v) ? this.config.client.createSet(v) : v
            )
        ),
        expr.del(
          this.config.update.delete &&
            mapValues(this.config.update.delete, v =>
              this.config.client.createSet(v)
            )
        )
      )
    )

    params.ReturnValues = ['NEW', 'OLD'].includes(this.config.return)
      ? `ALL_${this.config.return}`
      : this.config.return

    Object.assign(params, expr.merge(params as any, this.buildCondition()))

    if (!this.isComplete(params)) throw Error('incomplete update')

    super.log('update', params)
    const { Attributes } = await this.config.client.update(params).promise()
    const result = decode(
      this.config.return === 'NONE' ? undefined : Attributes
    )
    this.resolve(result as any)
  }

  remove(...fields: string[]) {
    const update = clone(this.config.update)
    ;(update.remove ??= []).push(...fields)
    return this.clone({ update })
  }

  add(fields: Exclude<Input<T>['add'], null>) {
    const update = clone(this.config.update)
    update.add = { ...update.add, ...fields }
    return this.clone({ update })
  }

  delete(fields: Exclude<Input<T>['delete'], null>) {
    const update = clone(this.config.update)
    update.delete = { ...update.delete, ...fields }
    return this.clone({ update })
  }

  returning<R extends ReturnType>(v: R): Update<T, R> {
    return this.clone({ return: v as any }) as any
  }

  ifExists() {
    let chain = this
    for (const [k, v] of Object.entries(this.config.key))
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
}
