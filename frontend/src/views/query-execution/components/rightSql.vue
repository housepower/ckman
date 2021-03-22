<template>
  <main>
    <section class="rightSql custom-scrollbar">
      <div class="tags flex-vcenter">
        <el-tag :class="['fs-14', 'mr-10', 'pointer', 'tag', 'flex', 'flex-vcenter', { active: activeIndex === index }]"
                type="info"
                v-for="(tag, index) in tagsList"
                :key="index"
                closable
                :disable-transitions="false"
                @close="handleClose(tag, index)"
                @click="clickTag(tag, index)">
          {{ tag.value.length < 10 ? `${tag.value.substr(0, 10)}` : `${tag.value.substr(0, 10)}...` }}
        </el-tag>
        <i class="fa fa-plus-square-o fs-28 pointer mr-10"
           @click="addTag"></i>
      </div>
    </section>

    <el-input type="textarea"
              ref="elInput"
              :autosize="{ minRows: 8 }"
              :disabled="!this.tagsList.length"
              v-model="sqlInput"
              class="width-full" />
    <el-button type="primary"
               size="large"
               class="fs-18 width-full mt-15"
               :disabled="sqlInput === ''"
               @click="query">Execute Query</el-button>
    <section class="list">
      <el-table :data="tableData"
                class="mt-15"
                border>
        <el-table-column show-overflow-tooltip
                         v-for="(item, index) of columns"
                         :key="index"
                         :prop="item"
                         :label="item" />
      </el-table>
    </section>
  </main>
</template>
<script>
import { remove } from "lodash-es";
import { SqlQuery } from "@/apis";
export default {
  props: {
    selectSql: {
      type: Object,
      default: {},
    },
  },
  data() {
    return {
      sqlInput: "",
      tagsList: [],
      activeIndex: 0,
      columns: [],
      tableData: [],
    };
  },
  mounted() {
    // this.addTag();
  },
  methods: {
    addTag() {
      this.sqlInput = "";
      this.tagsList.push({
        value: "",
        timestamp: new Date().getTime(),
      });
      this.activeIndex = this.tagsList.length - 1;
      this.$refs.elInput.focus();
    },
    handleClose(tag, index) {
      this.sqlInput = "";
      this.tagsList.splice(index, 1);
    },
    clickTag(tag, index) {
      this.activeIndex = index;
      this.sqlInput = tag.value;
      this.$refs.elInput.focus();
    },
    async query() {
      this.tableData = [];
      const {
        data: { entity },
      } = await SqlQuery.query({
        clusterName: this.$route.params.id,
        query: `${this.sqlInput}`,
      });
      if (entity.length) {
        this.columns = entity[0];
        entity.splice(0, 1);
        Object.values(entity).map((item, index) => {
          let dataItem = {};
          item.forEach((v, i) => {
            dataItem[this.columns[i]] = v;
          });
          this.tableData.push(dataItem);
        });
      }
      let curTag = this.tagsList.find(
        (item, index) => index === this.activeIndex
      );
      curTag.value = this.sqlInput;
      if (!curTag.value) {
        this.$message.warning("sql cannot be empty");
        return;
      }
      this.$nextTick(() => this.$emit("updateData", curTag));
    },
  },
  watch: {
    selectSql(item) {
      const isItemExist = this.tagsList.some((v) => v.value === item.value);
      if (!isItemExist) {
        this.tagsList.push(item);
        this.activeIndex = this.tagsList.length - 1;
      } else {
        this.activeIndex = this.tagsList.findIndex(
          (v) => v.value === item.value
        );
      }
      this.sqlInput = item.value;
      this.$refs.elInput.focus();
    },
  },
};
</script>

<style lang="scss" scoped>
.tags {
  padding: 5px 3px;

  .tag {
    width: 132px;
    justify-content: flex-end;

    &:hover,
    &.active {
      background: var(--primary-color);
      color: var(--color-white);
    }
  }

  i:hover {
    color: var(--primary-color);
  }
}
</style>
