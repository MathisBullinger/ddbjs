import * as naming from './utils/naming'
import surround from 'froebel/surround'

export interface Expression {
  ExpressionAttributeValues?: Record<string, any>
  ExpressionAttributeNames?: Record<string, string>
}

export interface UpdateExpression extends Expression {
  UpdateExpression: string
}

export interface ProjectionExpression extends Expression {
  ProjectionExpression: string
}

export interface ConditionExpression extends Expression {
  ConditionExpression: string
}

export interface FilterExpression extends Expression {
  FilterExpression: string
}

const build = <T extends Expression>(expr: T): T => {
  const result = { ...expr }
  Object.entries(result).forEach(([k, v]) => {
    if (typeof v === 'object' && Object.keys(v).length === 0)
      delete result[k as keyof Expression]
  })
  return result
}

const escapeName = (
  name: string,
  prefix: string,
  nameMap: Record<string, string>
) => {
  let key = ''
  for (const part of naming.parts(name)) {
    if (part.startsWith('[')) key += part
    else {
      let name = part
      if (!naming.valid(part)) {
        name = `#${prefix}${Object.keys(nameMap).length}`
        nameMap[name] = part
      }
      key += key.length ? `.${name}` : name
    }
  }
  return key
}

export const buildPairs = (
  input?: Record<string, any>,
  prefix = ''
): [Expression, [name: string, value: string, org: string][]] => {
  const pairs: [string, string, string][] = []
  const ExpressionAttributeValues: Expression['ExpressionAttributeValues'] = {}
  const ExpressionAttributeNames: Expression['ExpressionAttributeNames'] = {}

  const entries = Object.entries(input ?? {})

  if (entries.length) {
    for (let i = 0; i < entries.length; i++) {
      const [key, value] = entries[i]
      const av = `:${prefix}${i}`
      const name = escapeName(key, prefix, ExpressionAttributeNames)
      pairs.push([name, av, key])
      ExpressionAttributeValues[av] = value
    }
  }

  return [{ ExpressionAttributeValues, ExpressionAttributeNames }, pairs]
}

export const fun = Symbol('function')

type ExprOpts = {
  prefix?: string
  joiner?: string
}

const buildPairExpr =
  <T = any>(
    verb: string,
    { prefix = verb[0].toLowerCase(), joiner = ' ' }: ExprOpts = {}
  ) =>
  (input?: Record<string, T>): UpdateExpression | undefined => {
    const [expr, pairs] = buildPairs(input, prefix)

    if (!pairs.length) return
    return build({
      ...expr,
      UpdateExpression: `${verb} ${pairs
        .map(v => {
          let [key, value] = v
          const func = expr.ExpressionAttributeValues?.[value]?.[fun]
          if (func) {
            expr.ExpressionAttributeValues![value] =
              expr.ExpressionAttributeValues![value].data
            value = `${func}(${key}, ${value})`
          }
          return [key, value].join(joiner)
        })
        .join(', ')}`,
    })
  }

export const set = buildPairExpr('SET', { joiner: '=' })

export const add = buildPairExpr('ADD')

export const del = buildPairExpr('DELETE')

const escape =
  <T extends 'UpdateExpression' | 'ProjectionExpression'>(
    prefix: string,
    field: T,
    verb?: string
  ) =>
  (
    ...fields: string[]
  ):
    | (T extends 'UpdateExpression' ? UpdateExpression : ProjectionExpression)
    | undefined => {
    fields = Array.from(new Set(fields))
    if (!fields.length) return
    const names: string[] = []
    const ExpressionAttributeNames: Expression['ExpressionAttributeNames'] = {}

    for (const field of fields)
      names.push(escapeName(field, prefix, ExpressionAttributeNames))

    return build({
      [field]: (verb ? `${verb} ` : '') + names.join(', '),
      ExpressionAttributeNames,
    } as any)
  }

export const remove = escape('r', 'UpdateExpression', 'REMOVE')

export const project = escape('p', 'ProjectionExpression')

export const merge = (
  ...expressions: (
    | UpdateExpression
    | ProjectionExpression
    | ConditionExpression
    | FilterExpression
    | undefined
  )[]
): Expression => {
  const updateExprs: string[] = []
  const projectExprs: string[] = []
  const condExprs: string[] = []
  const ExpressionAttributeValues: Expression['ExpressionAttributeValues'] = {}
  const ExpressionAttributeNames: Expression['ExpressionAttributeNames'] = {}

  for (const e of expressions) {
    if (!e) continue
    if ('UpdateExpression' in e) updateExprs.push(e.UpdateExpression)
    if ('ProjectionExpression' in e) projectExprs.push(e.ProjectionExpression)
    if ('ConditionExpression' in e) condExprs.push(e.ConditionExpression)

    for (const [k, v] of Object.entries(e.ExpressionAttributeValues ?? {})) {
      if (k in ExpressionAttributeValues)
        throw Error(`duplicate key '${k}' in ExpressionAttributeValues`)
      ExpressionAttributeValues[k] = v
    }

    for (const [k, v] of Object.entries(e.ExpressionAttributeNames ?? {})) {
      if (k in ExpressionAttributeNames)
        throw Error(`duplicate key '${k} in ExpressionAttributeNames`)
      ExpressionAttributeNames[k] = v
    }
  }

  const expr: Expression & Record<string, string> = {}
  if (updateExprs.length) expr.UpdateExpression = updateExprs.join(' ')
  if (projectExprs.length) expr.ProjectionExpression = projectExprs.join(', ')
  if (condExprs.length) expr.ConditionExpression = condExprs.join(' AND ')
  if (Object.keys(ExpressionAttributeValues).length)
    expr.ExpressionAttributeValues = ExpressionAttributeValues
  if (Object.keys(ExpressionAttributeNames).length)
    expr.ExpressionAttributeNames = ExpressionAttributeNames
  return expr
}
