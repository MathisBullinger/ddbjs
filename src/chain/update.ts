import BaseChain from './base'
import * as build from '../expression'

type ReturnType = 'NONE' | 'OLD' | 'NEW' | 'UPDATED_OLD' | 'UPDATED_NEW'

type UpdateOpts = {
  table: string
  ifExists?: boolean
  key: any
}

export default class UpdateChain<
  T extends Schema<F>,
  F extends Fields = Omit<T, 'key'>
> extends BaseChain<F, any> {
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

    const updateExpressions: string[] = []
    const ExpressionAttributeValues: Record<string, any> = {}
    const ExpressionAttributeNames: Record<string, string> = {}

    this.update.set = this.makeSets(this.update.set)

    if (this.update.set) {
      const expr = build.set(this.update.set)
      updateExpressions.push(expr.UpdateExpression)
      Object.assign(ExpressionAttributeValues, expr.ExpressionAttributeValues)
      Object.assign(
        ExpressionAttributeNames,
        expr.ExpressionAttributeNames ?? {}
      )
    }

    if (Object.keys(ExpressionAttributeValues).length)
      params.ExpressionAttributeValues = ExpressionAttributeValues
    if (Object.keys(ExpressionAttributeNames).length)
      params.ExpressionAttributeNames = ExpressionAttributeNames

    params.UpdateExpression = updateExpressions.join(' ')

    if (!this.isComplete(params)) throw Error('incomplete update')

    await this.client.update(params).promise()

    this.resolve(undefined as any)
  }

  private isComplete(
    input: Partial<AWS.DynamoDB.DocumentClient.UpdateItemInput>
  ): input is AWS.DynamoDB.DocumentClient.UpdateItemInput {
    if (!input.UpdateExpression?.length)
      throw Error('missing update expression')
    return true
  }
}
