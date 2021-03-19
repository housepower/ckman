<template>
  <main class="login">
    <section class="container flex-center flex-column">
      <section>
        <h1 class="fs-24">Click House</h1>
        <h3 class="fs-20">Management Console</h3>
        <el-form :model="info"
                 status-icon
                 :rules="rules"
                 ref="Form"
                 label-width="80px"
                 class="pt-15">
          <el-form-item label="User"
                        prop="user">
            <el-input type="text"
                      v-model="info.user"
                      autocomplete="off"
                      class="width-300"></el-input>
          </el-form-item>
          <el-form-item label="Password"
                        prop="pass">
            <el-input type="password"
                      v-model="info.pass"
                      autocomplete="off"
                      class="width-300"></el-input>
          </el-form-item>
        </el-form>
        <el-button type="primary"
                   @click="login"
                   class="width-full">Login in</el-button>
      </section>
      <p style="position: absolute; bottom: -50px">Copyright © 2016-2020 上海擎创信息技术有限公司</p>
    </section>
  </main>
</template>

<script>
import { LoginApi } from "@/apis";
const md5 = require("blueimp-md5");
export default {
  data() {
    return {
      info: {
        pass: "",
        user: "",
      },
      rules: {
        pass: [
          { required: true, message: "Please input password", trigger: "blur" },
        ],
        user: [
          { required: true, message: "Please input user", trigger: "blur" },
        ],
      },
      redirect: "/home",
    };
  },
  mounted() {
    this.redirect = decodeURIComponent(this.$route.query.redirect || "/home");
  },
  methods: {
    async login() {
      await this.$refs.Form.validate();
      const {
        data: { entity },
      } = await LoginApi.login({
        password: md5(this.info.pass),
        username: this.info.user,
      });
      localStorage.setItem("user", JSON.stringify(entity));
      this.$root.userInfo = entity;
      this.$router.push({ path: this.redirect });
    },
  },
};
</script>

<style lang="scss" scoped>
.login {
  position: relative;
  top: 0;
  left: 0;
  bottom: 0;
  right: 0;
  height: 100vh;
  background: #cccccc52;
  .container {
    position: absolute;
    top: 50%;
    left: 50%;
    width: 450px;
    transform: translate3d(-50%, -50%, 0);
    padding: 20px 10px;
    background: #fff;
    border-radius: 3px;
    box-shadow: 0 0.3em 3em rgba(0, 0, 0, 0.1), 0 0 0 2px rgb(255, 255, 255),
      0.3em 0.3em 1em rgba(0, 0, 0, 0.3);

    h1,
    h3 {
      line-height: 50px;
      height: 50px;
      border-bottom: 2px solid #ebeef5;
      width: 100%;
      text-align: center;
    }
    h3 {
      height: 40px;
      line-height: 40px;
      border-bottom: 1px solid rgba(0, 0, 0, 0.5);
    }
  }
}
</style>
