export interface SharpSelectorDataModel {
  detail: {
    title: string;
    key: string;
    filterType: string;
    filterDisabled: boolean;
    [key: string]: any;
  };
  key: string;
  value: any;
  tempValue: any;
  visible: boolean;
  options?: {
    value: number | string | boolean;
    label: string;
    selected?: boolean;
  }[];
  searchText: string;
  displayText: string;
  displayTextArr: { label: string; value: string | number | boolean }[];
}

export interface SharpSelectorNewOptionsModel {
  title: string;
  key: string;
  filterType: string;
  filterDisabled?: boolean;
  [key: string]: any;
}

export interface SharpSelectorOptionDetailModel {
  [key: string]: { label: string; value: string | number | boolean }[];
}
