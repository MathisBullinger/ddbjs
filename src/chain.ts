interface Thenable<T = any> {
  then(cb: (v?: T) => void): void
}
type ThenCB<T> = Parameters<Thenable<T>['then']>[0]
type ReturnType = 'NONE' | 'NEW' | 'OLD'

type Item<T extends Fields> = { [K in keyof T]: SchemaValue<T[K]> }

abstract class Chain<
  TFields extends Fields,
  TReturn extends ReturnType = 'NONE',
  TResult = TReturn extends 'NONE' ? undefined : Item<TFields>
> implements Thenable<TResult> {
  constructor(
    protected readonly fields: TFields,
    protected readonly client: AWS.DynamoDB.DocumentClient,
    protected readonly params: AWS.DynamoDB.DocumentClient.PutItemInput,
    protected readonly returnType: TReturn
  ) {}

  public abstract then(cb: ThenCB<TResult>): void
}

export class PutChain<
  TFields extends Fields,
  TReturn extends ReturnType = 'NONE',
  TResult = TReturn extends 'NONE' ? undefined : Item<TFields>
> extends Chain<TFields, TReturn, TResult> {
  then(cb: ThenCB<TResult>) {
    this.client
      .put({
        ...this.params,
        ...(this.returnType === 'OLD' && {
          ReturnValues: 'ALL_OLD',
        }),
      })
      .promise()
  }

  returning<T extends ReturnType>(v: T): PutChain<TFields, T> {
    return new PutChain(this.fields, this.client, this.params, v)
  }
}
