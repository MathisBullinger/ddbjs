import BaseChain from './base'
import type { Fields } from '../types'
import * as expr from '../expression'
import * as naming from '../utils/naming'

type Comparator = '=' | '<>' | '<' | '<=' | '>' | '>='

type CompArgs<T, U> =
  | [field: keyof T & string, comparator: Comparator, value: any]
  | [cb: (chain: U) => U]

const isCB = <T, U>(args: CompArgs<T, U>): args is [(c: U) => U] =>
  typeof args[0] === 'function'

export default abstract class ConditionChain<
  T,
  F extends Fields
> extends BaseChain<T, F> {
  private ifAndOr =
    (con: 'AND' | 'OR') =>
    (...args: CompArgs<F, ConditionChain<T, F>>): this => {
      if (!isCB(args)) {
        this.addCondition(new Comparison(...args), con)
        return this
      } else {
        const cond = this.cloneConditon()
        delete this.condition
        const chain = args[0](this)
        chain.addCondition(cond, con, true)
        return chain as this
      }
    }

  public if = this.ifAndOr('AND')
  public andIf = this.ifAndOr('AND')
  public orIf = this.ifAndOr('OR')

  private addCondition(
    cond: ConditionList | undefined,
    con: 'AND' | 'OR',
    left = false
  ) {
    if (!cond) return
    if (!this.condition) this.condition = cond
    else
      this.condition = left
        ? [cond, con, this.condition]
        : [this.condition, con, cond]
  }

  protected condition?: ConditionList

  protected buildCondition(): expr.ConditionExpression | undefined {
    if (!this.condition) return

    const ExpressionAttributeNames: Record<string, any> = {}
    const ExpressionAttributeValues: Record<string, any> = {}

    const ConditionExpression = this.serialize(
      this.condition,
      name => {
        if (naming.valid(name)) return name
        const key = `#cn_${Object.keys(ExpressionAttributeNames).length}`
        ExpressionAttributeNames[key] = name
        return key
      },
      value => {
        const key = `:cv_${Object.keys(ExpressionAttributeValues).length}`
        ExpressionAttributeValues[key] = value
        return key
      }
    )

    return {
      ExpressionAttributeNames,
      ExpressionAttributeValues,
      ConditionExpression,
    }
  }

  private serialize(
    cond: ConditionList,
    name: (key: string) => string,
    value: (value: unknown) => string
  ): string {
    if (cond instanceof Condition) return cond.expr(name, value)
    return [cond[0], cond[2]]
      .map(v => `(${this.serialize(v, name, value)})`)
      .join(` ${cond[1]} `)
  }

  public cloneConditon(node = this.condition): ConditionList | undefined {
    if (!node) return
    if (node instanceof Condition) return node
    return [this.cloneConditon(node[0])!, node[1], this.cloneConditon(node[2])!]
  }
}

type ConditionList = Condition | [ConditionList, 'AND' | 'OR', ConditionList]

abstract class Condition {
  abstract expr(
    name: (key: string) => string,
    value: (value: unknown) => string
  ): string
}

class Comparison extends Condition {
  constructor(
    public readonly a: string,
    public readonly comp: Comparator,
    public readonly b: unknown
  ) {
    super()
  }

  expr(name: (key: string) => string, value: (value: unknown) => string) {
    return `${name(this.a)} ${this.comp} ${value(this.b)}`
  }
}
