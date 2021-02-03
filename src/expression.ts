import * as naming from './utils/naming'

interface Expression {
  ExpressionAttributeValues?: Record<string, any>
  ExpressionAttributeNames?: Record<string, string>
}

interface UpdateExpression extends Expression {
  UpdateExpression: string
}

interface ProjectionExpression extends Expression {
  ProjectionExpression: string
}

const build = <T extends Expression>(expr: T): T => {
  const result = { ...expr }
  Object.entries(result).forEach(([k, v]) => {
    if (typeof v === 'object' && Object.keys(v).length === 0)
      delete result[k as keyof Expression]
  })
  return result
}

const buildPairExpr = <T = any>(
  verb: string,
  joiner = ' ',
  prefix = verb[0].toLowerCase()
) => (input?: Record<string, T>): UpdateExpression | undefined => {
  if (!input || !Object.keys(input).length) return

  const pairs: [string, string][] = []
  const ExpressionAttributeValues: Expression['ExpressionAttributeValues'] = {}
  const ExpressionAttributeNames: Expression['ExpressionAttributeNames'] = {}

  const entries = Object.entries(input)

  for (let i = 0; i < entries.length; i++) {
    const [key, value] = entries[i]
    const av = `:${prefix}${i}`
    let name = key
    if (!naming.valid(name)) {
      name = `#${prefix}${Object.keys(ExpressionAttributeNames).length}`
      ExpressionAttributeNames[name] = key
    }
    pairs.push([name, av])
    ExpressionAttributeValues[av] = value
  }

  return build({
    UpdateExpression: `${verb} ${pairs.map(v => v.join(joiner)).join(', ')}`,
    ExpressionAttributeValues,
    ExpressionAttributeNames,
  })
}

export const set = buildPairExpr('SET', '=')

export const add = buildPairExpr('ADD')

export const del = buildPairExpr('DELETE')

const escape = <T extends 'UpdateExpression' | 'ProjectionExpression'>(
  prefix: string,
  field: T,
  verb?: string
) => (
  ...fields: string[]
):
  | (T extends 'UpdateExpression' ? UpdateExpression : ProjectionExpression)
  | undefined => {
  fields = Array.from(new Set(fields))
  if (!fields.length) return
  const names: string[] = []
  const ExpressionAttributeNames: Expression['ExpressionAttributeNames'] = {}

  for (const field of fields) {
    if (naming.valid(field)) names.push(field)
    else {
      const name = `#${prefix}${Object.keys(ExpressionAttributeNames).length}`
      names.push(name)
      ExpressionAttributeNames[name] = field
    }
  }

  return build({
    [field]: (verb ? `${verb} ` : '') + names.join(', '),
    ExpressionAttributeNames,
  } as any)
}

export const remove = escape('r', 'UpdateExpression', 'REMOVE')

export const project = escape('p', 'ProjectionExpression')

export const merge = (
  ...expressions: (UpdateExpression | ProjectionExpression | undefined)[]
): Expression => {
  const updateExprs: string[] = []
  const projectExprs: string[] = []
  const ExpressionAttributeValues: Expression['ExpressionAttributeValues'] = {}
  const ExpressionAttributeNames: Expression['ExpressionAttributeNames'] = {}

  for (const e of expressions) {
    if (!e) continue
    if ('UpdateExpression' in e) updateExprs.push(e.UpdateExpression)
    if ('ProjectionExpression' in e) projectExprs.push(e.ProjectionExpression)

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
  if (Object.keys(ExpressionAttributeValues).length)
    expr.ExpressionAttributeValues = ExpressionAttributeValues
  if (Object.keys(ExpressionAttributeNames).length)
    expr.ExpressionAttributeNames = ExpressionAttributeNames
  return expr
}
