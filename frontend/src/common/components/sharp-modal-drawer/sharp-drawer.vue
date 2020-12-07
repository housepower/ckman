<template>
  <el-drawer :modal-append-to-body="false"
             :focus-first="false"
             v-bind="props"
             :visible.sync="visible"
             :class="classes"
             @closed="onHidden"
             :before-close="onBeforeClose"
             ref="modal">
    <div class="sharp-drawer__body">
      <component :is="childComponent"
                 v-bind="data"
                 ref="body"
                 :modalInstance="this" />
    </div>
    <div class="footer" v-if="props.cancelText !== null || props.okText !== null">
      <el-button v-if="props.cancelText !== null"
                 @click="onCancel">{{ props.cancelText }}</el-button>
      <el-button v-if="props.okText !== null"
                 type="primary"
                 :loading="loading"
                 @click="onOk">{{ props.okText }}</el-button>
    </div>
  </el-drawer>
</template>
<script>
  import ModalDrawerMixin from './modal-drawer-mixin';

  export default {
    name: 'SharpDrawer',
    mixins: [ModalDrawerMixin],
  };
</script>
<style lang="scss" scoped>
  ::v-deep .el-drawer__body {
    display: flex;
    flex-direction: column;
  }

  .sharp-drawer__body {
    flex: 1;
    overflow-y: auto;
    padding: 20px;
  }

  .footer {
    background: white;
    border-top: solid 1px #E2E2E2;
    padding: 10px 20px 20px;
    text-align: right;
  }
</style>
