<template>
  <main class="login">
    <section class="container flex-center height-full">
      <el-form :model="info"
               status-icon
               :rules="rules"
               ref="Form"
               label-width="80px">
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
        <el-form-item>
          <el-button type="primary"
                     @click="login"
                     class="width-200">Login in</el-button>
        </el-form-item>
      </el-form>
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
        data: { data },
      } = await LoginApi.login({
        password: md5(this.info.pass),
        username: this.info.user,
      });
      localStorage.setItem("user", JSON.stringify(data));
      this.$root.userInfo = data;
      this.$router.push({ path: this.redirect });
    },
  },
};
</script>

<style lang="scss" scoped>
.login {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate3d(-50%, -50%, 0);
  width: 520px;
  height: 400px;
  background: linear-gradient(
    to right,
    rgba(201, 161, 0, 0.1),
    rgba(201, 161, 0, 0.2)
  );
}
</style>
