export const oneOf = <
  T extends string | number,
  K = T extends string ? string : number
>(
  v: T,
  ...arr: (T | K)[]
): boolean => arr.includes(v)
