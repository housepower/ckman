<template>
  <section class="json-editor-warp">
    <breadcrumb :data="['loader', 'manage']" />
    <div class="flex-end">
      <el-button type="primary"
                 size="large"
                 class="fs-18"
                 @click="submit">Submit</el-button>
    </div>
    <div class="flex height-full">
      <div class="json-editor flex-1 custom-scrollbar height-full"
           ref="jsonEditor"></div>
      <div class="flex-1 ml-10 custom-scrollbar height-full">
        <h4 class="mb-25">JSON OUTPUT</h4>
        <el-input type="textarea"
                  autosize
                  readonly
                  v-model="originStrinfyVal"
                  class="flex-1 ml-10" />
      </div>
    </div>

  </section>
</template>
<script>
import { JSONEditor } from "@json-editor/json-editor";
import { Schame, JsonValue } from "@/constants";
import "@/assets/style/bootstrap4.css";
export default {
  data() {
    return {
      schema: Schame,
      originValue: JsonValue,
      originStrinfyVal: JSON.stringify(JsonValue, null, 2),
      editor: null,
    };
  },
  mounted() {
    this.createInstence();
  },
  methods: {
    createInstence() {
      this.editor = new JSONEditor(this.$refs.jsonEditor, {
        theme: "bootstrap4",
        iconlib: "fontawesome4",
        schema: this.schema,
        startval: this.originValue,
        required_by_default: true,
        disable_array_reorder: true,
        no_additional_properties: true,
      });
      this.editor.on("change", () => {
        const errors = this.editor.validate();
        if (errors.length) {
          this.$message.error("json语法有误，请检查");
        } else {
          const json = this.editor.getValue();
          this.originStrinfyVal = JSON.stringify(json, null, 2);
        }
      });
    },
    async submit() {},
  },
  destroyed() {
    this.editor.destroy();
  },
};
</script>

<style lang="scss">
.json-editor-warp {
  overflow: hidden;
  height: 100vh;
  padding-bottom: 120px;
}
.json-editor {
  .card {
    border: none;
    border-left: 1px solid rgba(143, 69, 69, 0.125) !important;
  }
}
</style>