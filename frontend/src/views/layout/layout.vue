<template>
  <div class="layout">
    <header class="flex-between flex-vcenter plr-20">
      <span class="fs-18 font-bold">ClickHouse Management Console</span>
      <div class="header-right">
        <el-dropdown class="pointer">
          <div>
            <i class="fa fa-user-o fs-20"></i>
            <span v-text="user"
                  class="fs-16 ml-5 user" />
          </div>
          <el-dropdown-menu slot="dropdown">
            <el-dropdown-item @click.native="loginOut">Login out</el-dropdown-item>
          </el-dropdown-menu>
        </el-dropdown>
        <router-link to="/setting">
          <i class="fa fa-cog fs-20 pointer ml-10"></i>
        </router-link>
      </div>
    </header>

    <main class="plr-20 pt-10"
          style="padding-bottom: 85px">
      <router-view />
    </main>
    <transition name="el-fade-in-linear"
                appear>
      <footer class="flex-center width-full"
              v-show="$route.params.id">
        <div class="flex-center list-content width-1000">
          <router-link class="flex flex-1 flex-center height-full pointer list-item"
                       :to="{ path: item.path }"
                       exact-active-class="router-active"
                       v-for="item of menus"
                       :key="item.name">
            <span class="fs-20">{{ item.name }}</span>
          </router-link>
        </div>
      </footer>
    </transition>
  </div>
</template>
<script>
import { Menus, LoaderMenus } from "@/constants";
export default {
  name: "Layout",
  data() {
    return {
      menus: Menus,
      user: "",
    };
  },
  mounted() {
    this.user = JSON.parse(localStorage.getItem("user") || "{}").username;
  },
  methods: {
    handleMenuClick(e) {
      console.log(e);
    },
    loginOut() {
      localStorage.removeItem("user");
      this.$message.success("成功登出");
      setTimeout(() => {
        this.$router.push({ path: "/login" });
      }, 1000);
    },
  },
  watch: {
    $route: {
      handler: function (route, prevRoute) {
        this.menus = route.meta === "loader" ? LoaderMenus : Menus;
      },
      immediate: true,
    },
  },
};
</script>

<style lang="scss" scoped>
.layout {
  position: relative;
  height: 100%;
}
header {
  position: sticky;
  top: 0;
  z-index: 100;
  height: 50px;
  color: var(--color-white);
  background: var(--primary-color);

  .user,
  i {
    color: var(--color-white);
  }
}
footer {
  position: fixed;
  bottom: 0px;
  left: 50%;
  z-index: 100;
  transform: translateX(-50%);
  margin: 0 auto;
  background-color: #eaeef4;
  .list-content {
    height: 65px;

    .list-item {
      &:hover,
      &.router-active {
        background: var(--primary-color);
        transition: ease 0.5s;

        span {
          color: var(--color-white);
        }
      }
    }
  }
}
</style>
