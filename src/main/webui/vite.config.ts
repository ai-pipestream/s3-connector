import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'

export default defineConfig({
  plugins: [vue()],
  base: '/ui/',
  server: {
    host: 'localhost',
    port: 5177,
    strictPort: true,
  },
  build: {
    outDir: 'dist',
  },
})
