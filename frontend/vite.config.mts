import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/lands': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/grids': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/sentinel2': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/modis': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/weather': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/anomalies': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/forecast': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/dashboard': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/process': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/db-health': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
      '/health': {
        target: 'http://localhost:8000',
        changeOrigin: true,
      },
    },
  },
})
