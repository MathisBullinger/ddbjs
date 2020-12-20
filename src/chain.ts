import { oneOf } from './utils/array'
import { assert, ReturnValueError } from './utils/error'

interface Thenable<T = any> {
  then(cb: (v?: T) => void): void
}
type ThenCB<T> = Parameters<Thenable<T>['then']>[0]
type ReturnType = 'NONE' | 'NEW' | 'OLD' | 'UPDATED_OLD' | 'UPDATED_NEW'

type Item<T extends Fields> = { [K in keyof T]: SchemaValue<T[K]> }

abstract class Chain<
  TFields extends Fields,
  TReturn extends ReturnType = 'NONE',
  TResult = TReturn extends 'NONE' ? undefined : Item<TFields>
> implements Thenable<TResult> {
  constructor(
    protected readonly fields: TFields,
    protected readonly client: AWS.DynamoDB.DocumentClient,
    protected readonly returnType: TReturn
  ) {}

  public abstract then(cb: ThenCB<TResult>): void
}

export class PutChain<
  T1 extends Fields,
  T2 extends ReturnType = 'NONE',
  T3 = T2 extends 'NONE' ? undefined : Item<T1>
> extends Chain<T1, T2, T3> {
  constructor(
    fields: T1,
    client: AWS.DynamoDB.DocumentClient,
    private readonly params: AWS.DynamoDB.DocumentClient.PutItemInput,
    returnType: T2
  ) {
    super(fields, client, returnType)
  }

  then(cb: ThenCB<T3>) {
    this.client
      .put({
        ...this.params,
        ...(this.returnType === 'OLD' && {
          ReturnValues: 'ALL_OLD',
        }),
      })
      .promise()
      .then(({ Attributes }) => {
        if (this.returnType === 'NEW') cb(this.params.Item as T3)
        else if (this.returnType === 'OLD') cb(Attributes as T3)
        else cb()
      })
  }

  returning<T extends ReturnType>(v: T): PutChain<T1, T> {
    assert(oneOf(v, 'NEW', 'OLD', 'NONE'), new ReturnValueError(v, 'insert'))
    return new PutChain(this.fields, this.client, this.params, v)
  }
}

export class UpdateChain<
  T1 extends Fields,
  T2 extends ReturnType = 'NONE',
  T3 = T2 extends 'NONE' ? undefined : Item<T1>
> extends Chain<T1, T2, T3> {
  constructor(
    fields: T1,
    client: AWS.DynamoDB.DocumentClient,
    private readonly params: AWS.DynamoDB.DocumentClient.UpdateItemInput,
    returnType: T2
  ) {
    super(fields, client, returnType)
  }

  then(cb: ThenCB<T3>) {
    const params = this.params
    if (this.returnType.startsWith('UPDATED_'))
      this.params.ReturnValues = this.returnType
    else if (this.returnType !== 'NONE')
      this.params.ReturnValues = `ALL_${this.returnType}`
    this.client
      .update(params)
      .promise()
      .then(({ Attributes }) => {
        if (this.returnType !== 'NONE') cb(Attributes as T3)
        else cb()
      })
  }

  returning<T extends ReturnType>(v: T): UpdateChain<T1, T> {
    return new UpdateChain(this.fields, this.client, this.params, v)
  }
}
