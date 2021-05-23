import { DDB, DDBKey } from '../src'

test('build single key', () =>
  expect(
    (new DDB('test', { [DDBKey]: 'foo', foo: String }) as any).key('123')
  ).toMatchObject({ foo: '123' }))

test('build composite key', () =>
  expect(
    (new DDB('test', {
      [DDBKey]: ['foo', 'bar'],
      foo: String,
      bar: Number,
    }) as any).key('123', 123)
  ).toMatchObject({ foo: '123', bar: 123 }))

test('use "key" as key', () => {
  expect(
    (new DDB('test', { [DDBKey]: 'key', key: String }) as any).key('123')
  ).toMatchObject({ key: '123' })
  expect(
    (new DDB('test', {
      [DDBKey]: ['key', 'num'],
      key: String,
      num: Number,
    }) as any).key('123', 123)
  ).toMatchObject({ key: '123', num: 123 })
})
