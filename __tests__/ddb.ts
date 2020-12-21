import { DDB } from '../src'

test('build single key', () =>
  expect(
    (new DDB('test', { key: 'foo', foo: String }) as any).key('123')
  ).toMatchObject({ foo: '123' }))

test('build composite key', () =>
  expect(
    (new DDB('test', {
      key: ['foo', 'bar'],
      foo: String,
      bar: Number,
    }) as any).key('123', 123)
  ).toMatchObject({ foo: '123', bar: 123 }))

const str = `Lorem ipsum dolor sit amet, consectetur adipiscing elit. , / \\ - _ +`
test('encode & decode', () => expect(DDB.decode(DDB.encode(str))).toBe(str))
