import { defineConfig, loadEnv } from "vite";
import vue from "@vitejs/plugin-vue";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");

  return {
    base: env.VITE_APP_BASE || "/",
    plugins: [vue()],
    publicDir: false,
    build: {
      outDir: "../identity_validator/viewer_static",
      emptyOutDir: true,
    },
  };
});
