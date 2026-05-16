import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const api = process.env.API_URL ?? 'http://localhost:3000';

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      '/v1/ws': { target: api, ws: true },
      '/v1': api,
      '/demo': api,
      '/metrics': api,
    },
  },
});
