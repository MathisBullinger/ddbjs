import * as naming from './utils/naming'

export const set = (input: Record<string, any>) => {
  const pairs: [string, string][] = []
  const ExpressionAttributeValues: Record<string, any> = {}
  const ExpressionAttributeNames: Record<string, string> = {}

  const entries = Object.entries(input)

  for (let i = 0; i < entries.length; i++) {
    const [key, value] = entries[i]
    const av = `:s${i}`
    let name = key
    if (!naming.valid(name)) {
      name = `#s${Object.keys(ExpressionAttributeNames).length}`
      ExpressionAttributeNames[name] = key
    }
    pairs.push([name, av])
    ExpressionAttributeValues[av] = value
  }

  const UpdateExpression = 'SET ' + pairs.map(v => v.join('=')).join(', ')

  return {
    UpdateExpression,
    ExpressionAttributeValues,
    ...(Object.keys(ExpressionAttributeNames).length && {
      ExpressionAttributeNames,
    }),
  }
}
