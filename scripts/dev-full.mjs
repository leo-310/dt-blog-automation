import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");

const pythonExecutable =
  process.platform === "win32"
    ? path.join(rootDir, ".venv", "Scripts", "python.exe")
    : path.join(rootDir, ".venv", "bin", "python");

if (!existsSync(pythonExecutable)) {
  console.error("Missing virtual environment at .venv. Setup has not completed.");
  process.exit(1);
}

const children = new Set();
let stopping = false;

const apiProcess = spawn(
  pythonExecutable,
  ["-c", "from blog_agent.api import main; main()"],
  {
    cwd: rootDir,
    stdio: "inherit",
    env: process.env
  }
);
children.add(apiProcess);

const uiProcess = spawn(process.execPath, [path.join(__dirname, "dev-ui.mjs")], {
  cwd: rootDir,
  stdio: "inherit",
  env: process.env
});
children.add(uiProcess);

function terminateChildren() {
  if (stopping) return;
  stopping = true;
  for (const child of children) {
    if (!child.killed) {
      child.kill("SIGTERM");
    }
  }
  setTimeout(() => {
    for (const child of children) {
      if (!child.killed) {
        child.kill("SIGKILL");
      }
    }
  }, 1500);
}

function wireExit(child, name) {
  child.on("exit", (code, signal) => {
    children.delete(child);
    if (stopping) return;
    console.error(`${name} exited (${signal || code || 0}). Stopping dev:full.`);
    terminateChildren();
    process.exit(typeof code === "number" ? code : 1);
  });
}

wireExit(apiProcess, "API process");
wireExit(uiProcess, "UI process");

process.on("SIGINT", () => {
  terminateChildren();
  process.exit(0);
});

process.on("SIGTERM", () => {
  terminateChildren();
  process.exit(0);
});
