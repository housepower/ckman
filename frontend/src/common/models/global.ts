export interface ItoaResponse<T> {
  retCode: string;
  retMsg: string;
  entity: T;
  totalCount?: number;
  total?: number;
  [key: string]: any;
}
