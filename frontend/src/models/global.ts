export interface ItoaResponse<T> {
  retCode: string;
  retMsg: string;
  entity: T;
  totalCount: number;
  aux?: string;
  [key: string]: any;
}
