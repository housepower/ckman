<template>
  <section>
    <el-upload class="width-full"
               action="void"
               multiple
               :auto-upload="false"
               :file-list="fileList"
               :on-change="changeFile"
               :on-remove="removeFile"
               accept=".rpm">
      <el-button type="primary"
                 class="mb-15">Upload RPMs</el-button>
    </el-upload>
    <el-progress :text-inside="true"
                 :stroke-width="15"
                 :percentage="uploadPercent"
                 v-if="uploadPercent > 0" />
  </section>

</template>
<script>
import { PackageApi } from "@/apis";
export default {
  data() {
    return {
      fileList: [],
      uploadPercent: 0,
    };
  },
  mounted() {},
  methods: {
    changeFile(...rest) {
      this.fileList = rest[1];
    },
    removeFile(...rest) {
      this.fileList = rest[1];
    },
    async onOk() {
      if (this.fileList.length) {
        const resAll = this.fileList.map((file) => {
          let formData = new FormData();
          formData.append("package", file.raw);
          return PackageApi.upload(formData, {
            onUploadProgress: (progressEvent) => {
              const num =
                Math.floor((progressEvent.loaded / progressEvent.total) * 100) |
                0;
              this.uploadPercent = num;
            },
          });
        });
        await Promise.all(resAll);
        this.$message.success("rpm包上传成功");
      } else {
        this.$message.warning("请选取文件");
        return Promise.reject();
      }
    },
  },
};
</script>