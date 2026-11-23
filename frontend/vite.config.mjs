import { defineConfig } from 'vite';

export default defineConfig({
  server: {
    proxy: { '/api': process.env.QUEUEFORGE_API || 'http://127.0.0.1:8080' },
  },
});
