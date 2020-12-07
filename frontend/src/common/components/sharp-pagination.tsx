import Vue, { CreateElement } from 'vue';

import { PaginationConfig } from '../helpers';
import { Component, Prop, VueComponentBase, Watch } from '../VueComponentBase';
import { SharpDrawer, SharpModal } from './sharp-modal-drawer';

const defaultPageSize = 10;

@Component({
  name: 'SharpPagination',
})
export class SharpPagination extends VueComponentBase {
  @Prop() readonly pageConf: PaginationConfig;
  @Prop({
    default() {
      let current: Vue = this;
      while ((current = current.$parent)) {
        if ([SharpModal.name, SharpDrawer.name].includes(current.$options.name)) return null;
      }
      return { page: 'p', pageSize: 'ps' };
    }}) readonly urlParam;
  @Prop() readonly pageSizes = [5, 10, 20, 50, 100];
  @Prop() readonly layout = 'slot, total, prev, pager, next, sizes, jumper';

  @Watch('pageConf.totalItems')
  onTotalItemsChange(value: number) {
    const { itemsPerPage, currentPage } = this.pageConf;
    const maxPageNum = Math.max(Math.ceil(value / itemsPerPage), 1);
    if (maxPageNum < currentPage) this.change(maxPageNum);
  }

  @Watch('pageConf.currentPage')
  onCurrentPageChange(value: number) {
    if (!this.urlParam) return;
    const p = +this.$route.query.p || 1;
    if (value !== p) this.replaceUrl();
  }

  render(h: CreateElement) {
    return this.pageConf.totalItems ?
      <el-pagination
        class="text-right mt-10"
        layout={this.layout}
        onSize-change={this.pageSizeChange}
        onCurrent-change={this.change}
        current-page={this.pageConf.currentPage}
        page-size={this.pageConf.itemsPerPage}
        page-sizes={this.pageSizes}
        pager-count={5}
        total={this.pageConf.totalItems}>{this.$slots.default}</el-pagination> : null;
  }

  created() {
    if (this.pageConf.currentPage == null) {
      this.pageConf.currentPage = !this.urlParam ? 1 : +this.$route.query[this.urlParam.page] || 1;
    }
    if (this.pageConf.itemsPerPage == null) {
      this.pageConf.itemsPerPage = !this.urlParam ?
        defaultPageSize : +this.$route.query[this.urlParam.pageSize] || defaultPageSize;
    }
  }

  change(page: number) {
    this.pageConf.currentPage = page;
    this.replaceUrl();
    this.$emit('change', 'page');
  }

  replaceUrl() {
    if (!this.urlParam) return;
    const page = this.pageConf.currentPage === 1 ? null : this.pageConf.currentPage + '';
    // eslint-disable-next-line eqeqeq
    if (this.$route.query[this.urlParam.page] == page) return;
    this.$router.replace({
      query: {
        ...this.$route.query,
        [this.urlParam.page]: page,
      },
    });
  }

  pageSizeChange(pageSize: number) {
    this.pageConf.currentPage = 1;
    this.pageConf.itemsPerPage = pageSize;
    if (this.urlParam) {
      this.$router.replace({
        query: {
          ...this.$route.query,
          [this.urlParam.page]: null,
          [this.urlParam.pageSize]: this.pageConf.itemsPerPage === defaultPageSize
            ? null
            : this.pageConf.itemsPerPage + '',
        },
      });
    }
    this.$emit('change', 'pageSize');
  }
}
