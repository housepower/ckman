<template>
  <section class="left-aside-warp custom-scrollbar">
    <div class="left-aside-content">
      <ul class="custom-scrollbar">
        <li class="flex flex-column ptb-15 plr-10 flex-center">
          <span class="fs-18 font-bold mb-10">Query History</span>
          <el-input v-model="input"
                    placeholder="search"
                    autocomplete="false"
                    clearable
                    class="width-200"
                    @input="filterData"></el-input>
        </li>
        <li :class="['flex', 'flex-column', 'ptb-15', 'plr-10', 'flex-center', 'pointer', {'active': activeIndex === index}]"
            v-for="(item, index) of searchList"
            :key="index"
            @click="selectSql(item, index)">
          <span class="text-line-clamp"> {{ item.value }}</span>
        </li>
      </ul>
    </div>
  </section>
</template>
<script>
export default {
  props: {
    list: {
      type: Array,
      default: [],
    },
  },
  data() {
    return {
      input: "",
      searchList: this.list,
      activeIndex: -1,
    };
  },
  mounted() {},
  methods: {
    filterData() {
      if (this.input === "") this.searchList = this.list;
      this.searchList = this.searchList.filter((item) =>
        item.value.includes(this.input)
      );
    },
    selectSql(item, index) {
      this.activeIndex = index;
      this.$emit("selectSql", item);
    },
  },
  watch: {
    list(data) {
      this.searchList = data;
    },
  },
};
</script>

<style lang="scss" scoped>
.left-aside-warp {
  position: absolute;
  left: 20px;
  top: 105px;
  bottom: 65px;
  z-index: 50;
  width: 230px;
  border-right: 1px solid var(--color-gray);
  border-left: 1px solid var(--color-gray);
  .left-aside-content {
    ul {
      li {
        height: 125px;
        border-bottom: 1px solid var(--color-gray);
        letter-spacing: 1px;

        &:not(:first-child):hover {
          background: var(--primary-color);
          color: var(--color-white);
        }
      }
    }
  }
}
</style>
