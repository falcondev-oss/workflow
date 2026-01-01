// @ts-check
import eslintConfig from '@falcondev-oss/configs/eslint'

export default eslintConfig({
  tsconfigPath: './tsconfig.json',
}).append({
  ignores: [
    'node_modules/',
    'dist/',
    '.nuxt/',
    '.nitro/',
    '.output/',
    '.temp/',
    '.data/',
    'drizzle/',
    'prisma/generated/',
    'convex/_generated/',
    'pnpm-lock.yaml',
  ],
})
