export const oneOf = <
  T extends string | number,
  K = T extends string ? string : number
>(
  v: T,
  ...arr: (T | K)[]
): boolean => arr.includes(v)

export const batch = <T>(arr: T[], batchSize: number): T[][] =>
  Array(Math.ceil(arr.length / batchSize))
    .fill(0)
    .map((_, i) => arr.slice(i * batchSize, (i + 1) * batchSize))
