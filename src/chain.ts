import { oneOf } from './utils/array'
import { assert, ReturnValueError } from './utils/error'
import { decode } from './utils/convert'
import { mapKeys, mapValues } from './utils/object'
import { Bimap } from 'tsdast'

interface Thenable<T = any> {
  then(cb: (v?: T) => void, rej?: (reason?: any) => void): void
}
type ThenCB<T> = Parameters<Thenable<T>['then']>[0]
type ReturnType = 'NONE' | 'NEW' | 'OLD' | 'UPDATED_OLD' | 'UPDATED_NEW'

type Item<T extends Fields> = { [K in keyof T]: SchemaValue<T[K]> }

type Input =
  | AWS.DynamoDB.DocumentClient.PutItemInput
  | AWS.DynamoDB.DocumentClient.UpdateItemInput
  | AWS.DynamoDB.DocumentClient.DeleteItemInput

abstract class Chain<
  TFields extends Fields,
  TReturn extends ReturnType = 'NONE',
  TResult = TReturn extends 'NONE' ? undefined : Item<TFields>
> implements Thenable<TResult> {
  constructor(
    protected readonly fields: TFields,
    protected readonly client: AWS.DynamoDB.DocumentClient,
    protected readonly params: Input | undefined,
    protected readonly returnType: TReturn
  ) {}

  public abstract then(cb: ThenCB<TResult>, rej?: (reason?: any) => void): void

  protected clone(fields: TFields = this.fields): this {
    return new (<any>this).constructor(
      fields,
      this.client,
      this.params,
      this.returnType
    )
  }

  public cast(
    casts: ExplTypes<Item<TFields>> & { [k: string]: 'Set' | 'List' }
  ): this {
    return this.clone({
      ...this.fields,
      ...mapValues(casts, v => (v === 'Set' ? [String] : [])),
    })
  }

  protected isSet(attr: keyof TFields): boolean {
    const type = this.fields[attr]
    return Array.isArray(type) && [String, Number].includes(type[0] as any)
  }

  protected createSet(
    items: string[] | number[] | AWS.DynamoDB.DocumentClient.DynamoDbSet
  ): AWS.DynamoDB.DocumentClient.DynamoDbSet {
    if (!Array.isArray(items)) return items
    return this.client.createSet(items)
  }

  protected encode<T extends Record<string, any>>(raw: T): T {
    const encoded: Partial<T> = {}
    for (const [k, v] of Object.entries(raw) as [keyof T, any][]) {
      encoded[k] = this.isSet(k as any) ? this.createSet(v) : v
    }
    return encoded as T
  }

  protected static encodeKey(v: string) {
    return Buffer.from(v).toString('hex')
  }

  protected static decodeKey(v: string) {
    return Buffer.from(v, 'hex').toString()
  }

  protected compile(reject?: (error: Error) => void): boolean {
    try {
      if (!this.params) throw Error('params not specified')
      if (this.params.ExpressionAttributeValues) {
        const translation = Bimap.from(
          mapKeys(this.params.ExpressionAttributeNames as any, k =>
            (k as string).replace(/^#/, ':')
          )
        )

        this.params.ExpressionAttributeValues = {
          ...this.params.ExpressionAttributeValues,
          ...mapKeys(
            this.encode(
              mapKeys(
                this.params.ExpressionAttributeValues as any,
                k => translation.left[k as string]
              )
            ),
            k => translation.right[k as string]
          ),
        }
      }
      if ('Item' in this.params)
        this.params.Item = this.encode(this.params.Item)
    } catch (e) {
      if (reject) reject(e)
      else throw e
      return false
    }
    return true
  }
}

export class PutChain<
  T1 extends Fields,
  T2 extends ReturnType = 'NONE',
  T3 = T2 extends 'NONE' ? undefined : Item<T1>
> extends Chain<T1, T2, T3> {
  constructor(
    fields: T1,
    client: AWS.DynamoDB.DocumentClient,
    protected readonly params: AWS.DynamoDB.DocumentClient.PutItemInput,
    returnType: T2
  ) {
    super(fields, client, params, returnType)
  }

  then(resolve: ThenCB<T3>, reject: (reason: any) => void = () => {}) {
    if (!this.compile(reject)) return
    return this.client
      .put({
        ...this.params,
        ...(this.returnType === 'OLD' && {
          ReturnValues: 'ALL_OLD',
        }),
      })
      .promise()
      .then(({ Attributes }) => {
        if (this.returnType === 'NEW') resolve(decode(this.params.Item) as T3)
        else if (this.returnType === 'OLD') resolve(decode(Attributes) as T3)
        else resolve()
      })
      .catch(reject)
  }

  returning<T extends ReturnType>(v: T): PutChain<T1, T> {
    assert(oneOf(v, 'NEW', 'OLD', 'NONE'), new ReturnValueError(v, 'insert'))
    return new PutChain(this.fields, this.client, this.params, v)
  }
}

export class DeletionChain<
  T1 extends Fields,
  T2 extends ReturnType = 'NONE',
  T3 = T2 extends 'NONE' ? undefined : Item<T1>
> extends Chain<T1, T2, T3> {
  constructor(
    fields: T1,
    client: AWS.DynamoDB.DocumentClient,
    protected readonly params: AWS.DynamoDB.DocumentClient.DeleteItemInput,
    returnType: T2
  ) {
    super(fields, client, params, returnType)
  }

  then(resolve: ThenCB<T3>, reject: (reason: any) => void = () => {}) {
    if (!this.compile(reject)) return
    return this.client
      .delete({
        ...this.params,
        ...(this.returnType === 'OLD' && {
          ReturnValues: 'ALL_OLD',
        }),
      })
      .promise()
      .then(({ Attributes }) => {
        if (this.returnType === 'OLD') resolve(Attributes as T3)
        else resolve()
      })
      .catch(reject)
  }

  returning<T extends ReturnType>(v: T): DeletionChain<T1, T> {
    assert(oneOf(v, 'OLD', 'NONE'), new ReturnValueError(v, 'delete'))
    return new DeletionChain(this.fields, this.client, this.params, v)
  }
}

type UpdateOpts = {
  table: string
  ifExists?: boolean
  key: any
}

export class UpdateChain<
  T0 extends Schema<T1>,
  T1 extends Fields = Omit<T0, 'key'>,
  T2 extends ReturnType = 'NONE',
  T3 = T2 extends 'NONE' ? undefined : Item<T1>
> extends Chain<T1, T2, T3> {
  protected params:
    | AWS.DynamoDB.DocumentClient.UpdateItemInput
    | undefined = undefined

  constructor(
    fields: T1,
    client: AWS.DynamoDB.DocumentClient,
    private readonly update: UpdateInput<T0, T1> & UpdateOpts,
    returnType: T2
  ) {
    super(fields, client, undefined, returnType)
  }

  then(resolve: ThenCB<T3>, reject: (reason: any) => void = () => {}) {
    this.params = this.buildParams()
    if (!this.compile(reject) || !this.params) return

    if (this.returnType.startsWith('UPDATED_'))
      this.params.ReturnValues = this.returnType
    else if (this.returnType !== 'NONE')
      this.params.ReturnValues = `ALL_${this.returnType}`
    return this.client
      .update(this.params)
      .promise()
      .then(({ Attributes }) => {
        if (this.returnType !== 'NONE') resolve(decode(Attributes) as T3)
        else resolve()
      })
      .catch(reject)
  }

  returning<T extends ReturnType>(v: T): UpdateChain<T0, T1, T> {
    return new UpdateChain(this.fields, this.client, this.update, v)
  }

  ifExists(): UpdateChain<T0, T1, T2, T3> {
    return new UpdateChain(
      this.fields,
      this.client,
      { ...this.update, ifExists: true },
      this.returnType
    )
  }

  private buildParams(): AWS.DynamoDB.DocumentClient.UpdateItemInput {
    const ExpressionAttributeNames: Record<string, string> = {}
    const ExpressionAttributeValues: Record<string, any> = {}
    const updates: string[] = []

    const params: Partial<AWS.DynamoDB.DocumentClient.UpdateItemInput> = {
      TableName: this.update.table,
      Key: this.update.key,
    }

    if (this.update.set) {
      const sets: [string, string][] = []

      for (const [k, v] of Object.entries(this.update.set)) {
        const encKey = Chain.encodeKey(k)
        ExpressionAttributeNames[`#${encKey}`] = k
        ExpressionAttributeValues[`:${encKey}`] = v
        sets.push([`#${encKey}`, `:${encKey}`])
      }

      if (sets.length)
        updates.push(`SET ${sets.map(v => v.join('=')).join(', ')}`)
    }

    if (this.update.remove) {
      const remove: string[] = []

      for (const key of this.update.remove) {
        const encKey = '#' + Chain.encodeKey(key)
        ExpressionAttributeNames[encKey] = key
        remove.push(encKey)
      }

      if (remove.length) updates.push(`REMOVE ${remove.join(', ')}`)
    }

    if (this.update.ifExists && params?.Key) {
      params.ConditionExpression = Object.entries(params.Key)
        .map(([k, v]) => {
          ExpressionAttributeValues[`:${k}`] = v
          return `${k}=:${k}`
        })
        .join(' AND ')
    }

    return (Object.fromEntries(
      Object.entries({
        ...params,
        ExpressionAttributeNames,
        ExpressionAttributeValues,
        UpdateExpression: updates.join(' '),
      })
        .map(([k, v]) => {
          if (typeof v === 'object' && Object.keys(v).length === 0) return
          return [k, v]
        })
        .filter(Boolean) as any
    ) as unknown) as AWS.DynamoDB.DocumentClient.UpdateItemInput
  }

  protected clone(fields: T1 = this.fields): this {
    return new UpdateChain(
      fields,
      this.client,
      this.update,
      this.returnType
    ) as this
  }
}
