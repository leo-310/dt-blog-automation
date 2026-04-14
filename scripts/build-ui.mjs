import { transformAsync } from "@babel/core";
import commonjs from "@rollup/plugin-commonjs";
import { nodeResolve } from "@rollup/plugin-node-resolve";
import { rollup } from "rollup";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parse } from "yaml";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const distDir = path.join(rootDir, "dist");
const uiDir = path.join(rootDir, "src", "ui");
const dataDir = path.join(rootDir, "data");

export async function buildUi() {
  await rm(distDir, { recursive: true, force: true });
  await mkdir(distDir, { recursive: true });

  const [indexHtml, stylesCss, pipelineYaml, clustersYaml] = await Promise.all([
    readFile(path.join(rootDir, "index.html"), "utf8"),
    readFile(path.join(uiDir, "styles.css"), "utf8"),
    readFile(path.join(dataDir, "pipeline.yaml"), "utf8"),
    readFile(path.join(dataDir, "keyword_clusters.yaml"), "utf8")
  ]);

  const parsedPipeline = parse(pipelineYaml);
  const parsedClusters = parse(clustersYaml);
  const seedPipeline = Array.isArray(parsedPipeline?.pipeline) ? parsedPipeline.pipeline : [];
  const seedClusters = Array.isArray(parsedClusters?.clusters) ? parsedClusters.clusters : [];

  const bundle = await rollup({
    input: path.join(uiDir, "main.jsx"),
    plugins: [
      browserDataPlugin({ seedPipeline, seedClusters }),
      stripCssImportsPlugin(),
      jsxTransformPlugin(),
      nodeResolve({
        browser: true,
        extensions: [".mjs", ".js", ".jsx", ".json"]
      }),
      commonjs()
    ]
  });

  await bundle.write({
    file: path.join(distDir, "bundle.js"),
    format: "iife",
    name: "DoctorTowelsBlogAgentUI",
    intro: "var process = { env: { NODE_ENV: 'production' } };",
    sourcemap: false
  });
  await bundle.close();

  const builtIndexHtml = indexHtml
    .replace('/src/ui/styles.css', './styles.css')
    .replace('/src/ui/main.jsx', './bundle.js');

  await Promise.all([
    writeFile(path.join(distDir, "index.html"), builtIndexHtml),
    writeFile(path.join(distDir, "styles.css"), stylesCss)
  ]);
}

function jsxTransformPlugin() {
  return {
    name: "jsx-transform",
    async transform(code, id) {
      if (!id.endsWith(".jsx")) return null;
      const result = await transformAsync(code, {
        filename: id,
        babelrc: false,
        configFile: false,
        sourceMaps: false,
        presets: [["@babel/preset-react", { runtime: "automatic" }]]
      });
      return result?.code ? { code: result.code, map: null } : null;
    }
  };
}

function stripCssImportsPlugin() {
  return {
    name: "strip-css-imports",
    resolveId(source, importer) {
      if (source.endsWith(".css")) {
        return path.resolve(path.dirname(importer), source);
      }
      return null;
    },
    load(id) {
      if (id.endsWith(".css")) {
        return "export default undefined;";
      }
      return null;
    }
  };
}

function browserDataPlugin({ seedPipeline, seedClusters }) {
  const browserDataPath = path.join(uiDir, "lib", "browserData.js");
  const buildTimeApiBase = String(process.env.VITE_API_BASE_URL || "").trim().replace(/\/+$/, "");
  return {
    name: "browser-data-inline",
    load(id) {
      if (path.resolve(id) !== browserDataPath) return null;
      return readFile(browserDataPath, "utf8").then((source) => {
        let built = source;
        const importLines = [
          'import { parse } from "yaml";',
          'import pipelineRaw from "../../../data/pipeline.yaml?raw";',
          'import clustersRaw from "../../../data/keyword_clusters.yaml?raw";'
        ];
        for (const line of importLines) {
          built = built.replace(`${line}\r\n`, "").replace(`${line}\n`, "");
        }
        built = built.replace(
          [
            "const parsedPipeline = parse(pipelineRaw);",
            "const parsedClusters = parse(clustersRaw);",
            "const seedClusters = Array.isArray(parsedClusters?.clusters) ? parsedClusters.clusters : [];",
            "const seedPipeline = Array.isArray(parsedPipeline?.pipeline) ? parsedPipeline.pipeline : [];"
          ].join("\r\n"),
          `const seedClusters = ${JSON.stringify(seedClusters, null, 2)};\r\nconst seedPipeline = ${JSON.stringify(seedPipeline, null, 2)};`
        );
        built = built.replace(
          [
            "const parsedPipeline = parse(pipelineRaw);",
            "const parsedClusters = parse(clustersRaw);",
            "const seedClusters = Array.isArray(parsedClusters?.clusters) ? parsedClusters.clusters : [];",
            "const seedPipeline = Array.isArray(parsedPipeline?.pipeline) ? parsedPipeline.pipeline : [];"
          ].join("\n"),
          `const seedClusters = ${JSON.stringify(seedClusters, null, 2)};\nconst seedPipeline = ${JSON.stringify(seedPipeline, null, 2)};`
        );
        built = built.replaceAll("__VITE_API_BASE_URL__", buildTimeApiBase);
        if (built.includes("?raw") || built.includes('from "yaml"')) {
          throw new Error("browserData.js still contains non-browser imports");
        }
        return built;
      });
    }
  };
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await buildUi();
  console.log("Static UI build complete: dist/");
}
