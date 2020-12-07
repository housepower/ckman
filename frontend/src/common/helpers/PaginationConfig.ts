export class PaginationConfig {
  currentPage = null;
  totalItems = 0;

  constructor(public itemsPerPage = null) {}

  toApiParams(): { page: number; from: number; size: number } {
    const page = (this.currentPage || 1) - 1;
    const size = this.itemsPerPage || 10;
    return {
      page,
      from: page * size,
      size,
    };
  }

  filterData<T>(arr: T[]) {
    const { from, size } = this.toApiParams();
    return arr.slice(from, from + size);
  }
}
