import { JSDOM, VirtualConsole } from "jsdom";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const distDir = path.join(rootDir, "dist");

const html = await readFile(path.join(distDir, "index.html"), "utf8");
const bundle = await readFile(path.join(distDir, "bundle.js"), "utf8");

const virtualConsole = new VirtualConsole();
virtualConsole.on("error", (message) => console.error("console.error:", message));
virtualConsole.on("warn", (message) => console.warn("console.warn:", message));
virtualConsole.on("jsdomError", (error) => console.error("jsdomError:", error));

const dom = new JSDOM(html, {
  url: "http://127.0.0.1:4173/",
  runScripts: "dangerously",
  pretendToBeVisual: true,
  virtualConsole
});

dom.window.addEventListener("error", (event) => {
  console.error("window.error:", event.error || event.message);
});

try {
  dom.window.eval(bundle);
} catch (error) {
  console.error("eval.error:", error);
}

await new Promise((resolve) => setTimeout(resolve, 1000));

console.log("root html length:", dom.window.document.getElementById("root")?.innerHTML.length ?? -1);
console.log("root preview:", dom.window.document.getElementById("root")?.innerHTML.slice(0, 500) ?? "");
