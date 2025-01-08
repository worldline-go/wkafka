import * as path from "path";
import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { createHtmlPlugin } from "vite-plugin-html";

// https://vitejs.dev/config/
export default defineConfig({
  base: process.env.NODE_ENV == "production" ? "./" : "/wkafka/ui/",
  plugins: [
    svelte(),
    createHtmlPlugin({
      minify: process.env.NODE_ENV == "production",
    }),
  ],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
    },
  },
  build: {
    sourcemap: process.env.NODE_ENV == "production" ? false : true,
  },
  server: {
    proxy: {
      "/wkafka/v1": {
        target: "http://localhost:8080",
        changeOrigin: true,
        secure: true,
        ws: true,
        followRedirects: true,
      },
      "/wkafka/files": {
        target: "http://localhost:8080",
        changeOrigin: true,
        secure: true,
        ws: true,
        followRedirects: true,
      },
    },
    port: process.env.PORT ?? 3000,
  },
});
