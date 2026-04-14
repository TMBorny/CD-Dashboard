const js = require("@eslint/js");
const globals = require("globals");
const tsPlugin = require("@typescript-eslint/eslint-plugin");
const tsParser = require("@typescript-eslint/parser");
const vuePlugin = require("eslint-plugin-vue");
const vueParser = require("vue-eslint-parser");

module.exports = [
  {
    ignores: ["dist/**", "node_modules/**", "client-health-backend/.venv/**"],
  },
  js.configs.recommended,
  ...tsPlugin.configs["flat/recommended"],
  ...vuePlugin.configs["flat/essential"],
  {
    files: ["**/*.{js,mjs,ts,vue}"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "module",
      globals: {
        ...globals.browser,
        ...globals.node,
      },
      parserOptions: {
        parser: tsParser,
      },
    },
    rules: {
      "@typescript-eslint/no-explicit-any": "off",
      "@typescript-eslint/no-require-imports": "off",
      "vue/multi-word-component-names": "off",
    },
  },
  {
    files: ["**/*.cjs"],
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "commonjs",
      globals: globals.node,
    },
    rules: {
      "@typescript-eslint/no-require-imports": "off",
    },
  },
  {
    files: ["eslint.config.cjs"],
    rules: {
      "@typescript-eslint/no-require-imports": "off",
    },
  },
  {
    files: ["**/*.vue"],
    languageOptions: {
      parser: vueParser,
      parserOptions: {
        parser: tsParser,
        ecmaVersion: "latest",
        sourceType: "module",
        extraFileExtensions: [".vue"],
      },
    },
  },
];
