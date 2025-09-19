// filepath: /C:/Users/there/Documents/GitHub/documentation/tailwind.config.js
import { type Config } from "tailwindcss";

export default {
  darkMode: 'class',
  content: [
    "./pages/**/*.{js,ts,jsx,tsx}",
    "./components/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        "gb-primary": "#d12f53",
        "gb-secondary": "#00aeff",
        "gb-bg1" : "#1e1e20",
        "gb-bg2" : "#2a2a2d",
        "gb-bg3" : "#121212",
        "gb-danger1": "#6b0002",
        "gb-danger2": "#ff0000",
        "gb-faded": "#bcc0c0",
        "gb-light-bg1" : "#ffffff",
        "gb-light-bg2" : "#ededed",
        "gb-light-bg3" : "#999999",
        "gb-text-dark" : "#334155",
      }
    },
  },
  plugins: [],
} satisfies Config;