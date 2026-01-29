import type { Plugin } from 'rolldown'
import fs from 'node:fs/promises'
import { createRequire } from 'node:module'
import path from 'node:path'
import { defineConfig, globalLogger } from 'tsdown'

export default defineConfig({
  entry: ['./src/index.ts'],
  outDir: './dist',
  format: 'esm',
  dts: true,
  noExternal: ['groupmq'], // bundled so we can inline Lua scripts
  plugins: [inlineGroupMQLuaPlugin()],
})

export function inlineGroupMQLuaPlugin(): Plugin {
  const require = createRequire(import.meta.url)

  const luaScriptsByName: Record<string, string> = {}

  return {
    name: 'inline-groupmq-lua',
    async buildStart() {
      const packageJsonPath = require.resolve('groupmq/package.json')
      const root = path.dirname(packageJsonPath)

      const files = await getLuaFiles(root)

      for (const filePath of files) {
        const content = await fs.readFile(filePath, 'utf8')
        const name = path.basename(filePath, '.lua')
        luaScriptsByName[name] = content
      }

      globalLogger.info('Found Lua scripts from groupmq:', Object.keys(luaScriptsByName))
    },
    transform(code, id) {
      if (!id.includes('groupmq') || !code.includes('function scriptPath(name)')) {
        return null
      }

      globalLogger.info('Transforming groupmq script loader:', id)

      const scriptsJson = JSON.stringify(luaScriptsByName)
      const inlinedScriptsCode = `const __INLINED_LUA_SCRIPTS__ = ${scriptsJson};\n`

      let newCode = inlinedScriptsCode + code

      // original line: const lua = fs.readFileSync(file, "utf8");
      const readRegex = /const\s+lua\s*=\s*fs\.readFileSync\s*\(\s*file\s*,\s*["']utf8["']\s*\);/g

      if (readRegex.test(newCode)) {
        newCode = newCode.replaceAll(
          readRegex,
          `const lua = __INLINED_LUA_SCRIPTS__[name]; 
        if (!lua) throw new Error("Lua script " + name + " not found in bundle");`,
        )

        newCode = newCode.replaceAll(
          /const\s+file\s*=\s*scriptPath\s*\(\s*name\s*\);/g,
          '// const file = scriptPath(name); -- removed by bundler',
        )
      } else {
        throw new Error('Failed to find fs.readFileSync usage in groupmq Lua loader')
      }

      return {
        code: newCode,
        map: null,
      }
    },
  }
}

async function getLuaFiles(dir: string) {
  const entries = await fs.readdir(dir, { withFileTypes: true })
  const files = [] as string[]

  for (const entry of entries) {
    const fullPath = path.join(dir, entry.name)

    if (entry.isDirectory()) {
      files.push(...(await getLuaFiles(fullPath)))
    } else if (entry.isFile() && entry.name.endsWith('.lua')) {
      files.push(fullPath)
    }
  }

  return files
}
