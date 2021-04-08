import BaseChain from './base'
import * as build from '../expression'
import { decode } from '../utils/convert'
import type { Fields, Schema, DBItem, UpdateInput } from '../types'
import { mapValues } from '../utils/object'

type ReturnType = 'NONE' | 'OLD' | 'NEW' | 'UPDATED_OLD' | 'UPDATED_NEW'

type UpdateOpts = {
  table: string
  ifExists?: boolean
  key: any
}

export class UpdateChain<
  T extends Schema<F>,
  RT extends ReturnType,
  F extends Fields = Omit<T, 'key'>,
  RV = RT extends 'NONE'
    ? undefined
    : RT extends 'OLD' | 'NEW'
    ? DBItem<F>
    : Partial<DBItem<F>>
> extends BaseChain<RV, F> {
  constructor(
    fields: F,
    client: AWS.DynamoDB.DocumentClient,
    private readonly update: UpdateInput<T, F> & UpdateOpts,
    private readonly returnType: ReturnType = 'NONE'
  ) {
    super(fields, client)
  }

  async execute() {
    const params: Partial<AWS.DynamoDB.DocumentClient.UpdateItemInput> = {
      TableName: this.update.table,
      Key: this.update.key,
    }
    const conditions: string[] = []

    this.update.set = this.makeSets(this.update.set)

    Object.assign(
      params,
      build.merge(
        build.set(this.update.set),
        build.remove(...(this.update.remove ?? [])),
        build.add(
          this.update.add &&
            mapValues(this.update.add, v =>
              Array.isArray(v) ? this.client.createSet(v) : v
            )
        ),
        build.del(
          this.update.delete &&
            mapValues(this.update.delete, v => this.client.createSet(v))
        )
      )
    )

    params.ReturnValues = ['NEW', 'OLD'].includes(this.returnType)
      ? `ALL_${this.returnType}`
      : this.returnType

    if (this.update.ifExists) {
      for (const [k, v] of Object.entries(params.Key!)) {
        const name = `:${k}`
        ;(params.ExpressionAttributeValues ??= {})[name] = v
        conditions.push(`${k}=${name}`)
      }
    }

    if (conditions.length) params.ConditionExpression = conditions.join(' AND ')

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
    return this.clone(this.fields, update)
  }

  add(
    fields: Exclude<UpdateInput<T, F>['add'], null>
  ): UpdateChain<T, RT, F, RV> {
    const update = this.update
    update.add = { ...update.add, ...fields }
    return this.clone(this.fields, update)
  }

  delete(
    fields: Exclude<UpdateInput<T, F>['delete'], null>
  ): UpdateChain<T, RT, F, RV> {
    const update = this.update
    update.delete = { ...update.delete, ...fields }
    return this.clone(this.fields, update)
  }

  returning<R extends ReturnType>(v: R): UpdateChain<T, R, F> {
    return new UpdateChain(this.fields, this.client, this.update, v)
  }

  ifExists(): UpdateChain<T, RT, F, RV> {
    return new UpdateChain(
      this.fields,
      this.client,
      { ...this.update, ifExists: true },
      this.returnType
    )
  }

  private isComplete(
    input: Partial<AWS.DynamoDB.DocumentClient.UpdateItemInput>
  ): input is AWS.DynamoDB.DocumentClient.UpdateItemInput {
    if (!input.UpdateExpression?.length)
      throw Error('missing update expression')
    return true
  }

  public cast = super._cast.bind(this)

  protected clone(fields = this.fields, update = this.update) {
    return new UpdateChain(fields, this.client, update, this.returnType) as any
  }
}
