import { TimeFilterType } from './timeFilter';

export interface SplApiParamModel {
  _permission?: boolean;
  timefilter?: TimeFilterType;
  start?: string | number;
  end?: string | number;
  highlight?: boolean;
  sortBy?: { [key: string]: 'asc' | 'desc' }[];
  asc?: boolean;
  pageConf?: {
    currentPage: number;
    itemsPerPage: number;
  };
  from?: number;
  size?: number;
  time_zone?: string;
  filters?: string[];
  format?: 'std' | 'tree';
  includes?: string;
}

export interface SplAggsModel {
  aggs?: any[];
  buckets?: any[];
  dateKeys: string[];
  valueKeys?: string[];
  otherKeys?: string[];
  values?: any[];
  fields: string[];
  keys: string[];
  total: number;
}

export interface SplEventsModel {
  events: any[];
  fields: string[];
  total: number;
}

export interface SplResultModel {
  aggs: SplAggsModel;
  events: SplEventsModel;
  type: 'aggs' | 'events';
  error?: string;
  error_raw?: string;
  detail?: string;
}

export interface SplQueryFieldsResultModel {
  field_stats: {
    [key: string]: {
      count: number;
      type: 'keyword' | 'text' | 'long' | 'date' | string;
    };
  };
  fields: null;
}

export interface SplExplainDrillResultModel {
  query: string;
}
