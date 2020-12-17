import * as AWS from 'aws-sdk'

export type Schema<T extends Fields> = T & { key: Key<T> }
export type Fields = Record<string, SchemaValueType>
type SchemaValueType = StringConstructor | NumberConstructor
export type Key<T extends Fields> = keyof T | CompositeKey<T>
export type CompositeKey<T extends Fields> = [hash: keyof T, sort: keyof T]
type SchemaValue<T extends SchemaValueType> = T extends StringConstructor
  ? string
  : number

type KeyValue<
  T extends Schema<F>,
  F extends Fields = Omit<T, 'key'>
> = T['key'] extends CompositeKey<F>
  ? [SchemaValue<F[T['key'][0]]>, SchemaValue<F[T['key'][1]]>]
  : T['key'] extends keyof F
  ? [SchemaValue<F[T['key']]>]
  : never

type KeyFields<T extends Fields, K extends Key<T>> = keyof Pick<
  T,
  K extends CompositeKey<T> ? K[0] | K[1] : K
>

type Item<TFields extends Fields, TKey extends Key<TFields>> = {
  [K in KeyFields<TFields, TKey>]: SchemaValue<TFields[K]>
} &
  {
    [K in keyof Omit<TFields, KeyFields<TFields, TKey>>]?: SchemaValue<
      TFields[K]
    >
  }

interface Thenable<T = any> {
  then(cb: (v?: T) => void): void
}

class PutChain<T = undefined> implements Thenable<T> {
  constructor(
    private readonly client: AWS.DynamoDB.DocumentClient,
    private readonly params: AWS.DynamoDB.DocumentClient.PutItemInput,
    private readonly returnValue: 'NONE' | 'OLD' | 'NEW' = 'NONE'
  ) {}

  then(cb: (v?: T) => void) {
    this.client
      .put({
        ...this.params,
        ...(this.returnValue === 'OLD' && {
          ReturnValues: 'ALL_OLD',
        }),
      })
      .promise()
      .then(({ Attributes }) => {
        if (this.returnValue === 'OLD') cb(Attributes as T)
        else if (this.returnValue === 'NEW') cb(this.params.Item as T)
        else cb()
      })
  }

  returning(v: 'OLD' | 'NEW') {
    return new PutChain(this.client, this.params, v)
  }
}

export class DDB<T extends Schema<F>, F extends Fields = Omit<T, 'key'>> {
  public readonly client: AWS.DynamoDB.DocumentClient

  constructor(
    public readonly table: string,
    private readonly schema: T,
    opts?: ConstructorParameters<typeof AWS.DynamoDB.DocumentClient>[0]
  ) {
    this.client = new AWS.DynamoDB.DocumentClient(opts)
  }

  public async get(
    ...key: KeyValue<T, F>
  ): Promise<Item<F, T['key']> | undefined> {
    const { Item } = await this.client
      .get({
        TableName: this.table,
        Key: this.key(...key),
      })
      .promise()

    return Item as any
  }

  public insert(item: Item<F, T['key']>) {
    const params: AWS.DynamoDB.DocumentClient.PutItemInput = {
      TableName: this.table,
      Item: item,
    }
    return new PutChain(this.client, params)
  }

  private key(...v: KeyValue<T, F>) {
    return Object.fromEntries(
      (typeof this.schema.key === 'string'
        ? [this.schema.key]
        : (this.schema.key as string[])
      ).map((k, i) => [k, v[i]])
    )
  }
}
