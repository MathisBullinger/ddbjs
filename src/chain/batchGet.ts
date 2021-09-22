import BaseChain, { Config } from './base'
import { decode } from '../utils/convert'
import * as expr from '../expression'
import { batch } from '../utils/array'
import type { Schema, Fields, ScFields, Projected, Field } from '../types'

type BatchGetConfig<T extends Schema<any>> = Config<T> & {
  keys: any[]
  sort?: boolean
  selection?: any[]
  removeFields?: any[]
}

export class BatchGet<
  T extends Schema<any>,
  S extends string | number | symbol = Field<T>
> extends BaseChain<Projected<ScFields<T>, S>[], BatchGetConfig<T>> {
  constructor(config: BatchGetConfig<T>) {
    super(config, {})
  }

  async execute() {
    const selected = [...(this.config.selection ?? [])]

    const params = expr.project(...selected)

    const items = (
      await Promise.all(
        batch(this.config.keys, 100).map(v => this.read(v, params as any))
      )
    ).flat()

    this.resolve(
      this.removeKeys(
        this.config.sort ? BatchGet.sortByKeys(this.config.keys, items) : items
      ).map(decode) as any
    )
  }

  public select<Fields extends string>(
    ...fields: Fields[]
  ): BatchGet<T, Fields> {
    return this.clone({ selection: fields }) as any
  }

  private async read(
    Keys?: any[],
    params?: Partial<AWS.DynamoDB.BatchGetItemInput>
  ): Promise<any[]> {
    if (!Keys?.length) return []

    const payload = {
      RequestItems: {
        [this.config.table]: {
          Keys,
          ...params,
        },
      },
    }
    super.log('batchGet', payload)

    const { Responses, UnprocessedKeys } = await this.config.client
      .batchGet(payload)
      .promise()

    return [
      ...(Responses?.[this.config.table] ?? []),
      ...(await this.read(UnprocessedKeys?.[this.config.table]?.Keys)),
    ]
  }

  public sort() {
    const keyFields = [
      (this.config.schema as Schema<Fields>)[BaseChain.key!],
    ].flat()
    return this.clone({
      sort: true,
      ...(this.config.selection && {
        selection: [...this.config.selection, ...keyFields],
        removeFields: keyFields.filter(
          v => !this.config.selection!.includes(v)
        ),
      }),
    })
  }

  private removeKeys<T>(items: T[]): T[] {
    if (!this.config.removeFields?.length) return items
    return items.map(
      v =>
        Object.fromEntries(
          Object.entries(v).map(([k, v]) =>
            this.config.removeFields!.includes(k) ? [] : [k, v]
          )
        ) as T
    )
  }

  private static sortByKeys(keys: any[], items: any[]) {
    const remaining = [...items]
    const sorted = []

    for (const key of keys) {
      const props = Object.entries(key)
      const i = remaining.findIndex(item =>
        props.every(([k, v]) => item[k] === v)
      )
      if (i === -1) continue
      sorted.push(remaining.splice(i, 1)[0])
    }

    sorted.push(...remaining)
    return sorted
  }
}
