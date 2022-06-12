import { db, ranId, TableName } from './utils/db'
import * as https from 'https'
import { valid, parts } from '../src/utils/naming'
import reserved from '../src/reserved'

const byteLimit = 64 * 1024 - 1

const put = async (item: any) =>
  await db.client.put({ TableName, Item: { id: ranId(), ...item } }).promise()

test(`exceed ${byteLimit} bytes limit`, async () => {
  const key = 'a'.repeat(byteLimit)

  await expect(put({ [key]: 'ok' })).resolves.not.toThrow()

  await expect(
    put({
      TableName,
      Item: { [key + 'a']: 'too long' },
    })
  ).rejects.toThrow()
})

test.skip('reserved keywords', async () => {
  const html = await new Promise<string>(resolve =>
    https.get(
      'https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html',
      res => {
        res.setEncoding('utf8')
        let txt = ''
        res.on('data', v => {
          txt += v
        })
        res.on('end', () => {
          resolve(txt)
        })
      }
    )
  )

  const list = html
    .split('</code>')[0]
    .split('>')
    .pop()
    ?.split('\n')
    .map(v => v.trim())
    .filter(Boolean)
  if (!list) throw Error("couldn't parse reserved word list")

  expect(reserved).toEqual(list)

  expect(valid('')).toBe(false)
  expect(valid('asdf')).toBe(true)

  const split = (v: string, i: number) => [v.slice(0, i), v.slice(i)]
  for (let word of list) {
    const [a, b] = split(word, list.indexOf(word) % word.length)
    word = a.toLowerCase() + b.toUpperCase()
    expect(valid(word)).toBe(false)
  }
})

test('split path', () => {
  expect(parts('foo.bar.baz')).toEqual(['foo', 'bar', 'baz'])
  expect(parts('foo[bar].baz')).toEqual(['foo', '[bar]', 'baz'])
  expect(parts('foo.bar[baz]')).toEqual(['foo', 'bar', '[baz]'])
  expect(parts('foo[bar][baz]')).toEqual(['foo', '[bar]', '[baz]'])
})
