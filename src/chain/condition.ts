import BaseChain from './base'
import type { Fields } from '../types'
import * as expr from '../expression'

type Comparator = '=' | '<>' | '<' | '<=' | '>' | '>='

export default abstract class ConditionChain<
  T,
  F extends Fields
> extends BaseChain<T, F> {
  public if(field: keyof F, comparator: Comparator, value: any): this {
    this.conditions.push([field as string, comparator, value])
    return this
  }

  protected conditions: [string, Comparator, any][] = []

  protected buildCondition(): expr.ConditionExpression | undefined {
    if (!this.conditions.length) return
    const [ex, fields] = expr.buildPairs(
      Object.fromEntries(this.conditions.map(([k, _, v]) => [k, v]))
    )
    const fieldMap = Object.fromEntries(fields.map(([n, v, o]) => [o, [n, v]]))

    const ConditionExpression = this.conditions
      .map(([f, c]) => `${fieldMap[f][0]} ${c} ${fieldMap[f][1]}`)
      .join(' AND ')

    return { ...ex, ConditionExpression }
  }
}
