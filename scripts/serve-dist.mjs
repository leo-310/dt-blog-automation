import http from "node:http";
import { createReadStream, existsSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, "..");
const distDir = path.join(rootDir, "dist");
const port = Number(process.env.PORT || 4173);
const host = process.env.HOST || "127.0.0.1";

const contentTypes = new Map([
  [".html", "text/html; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".css", "text/css; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".svg", "image/svg+xml"],
  [".png", "image/png"],
  [".jpg", "image/jpeg"],
  [".jpeg", "image/jpeg"]
]);

const server = http.createServer((req, res) => {
  const urlPath = decodeURIComponent((req.url || "/").split("?")[0]);
  const relativePath = urlPath === "/" ? "index.html" : urlPath.replace(/^\/+/, "");
  const filePath = path.join(distDir, relativePath);
  const fallbackPath = path.join(distDir, "index.html");
  const targetPath = existsSync(filePath) ? filePath : fallbackPath;
  const ext = path.extname(targetPath).toLowerCase();

  res.setHeader("Content-Type", contentTypes.get(ext) || "application/octet-stream");
  createReadStream(targetPath)
    .on("error", () => {
      res.statusCode = 500;
      res.end("Unable to read file.");
    })
    .pipe(res);
});

server.listen(port, host, () => {
  console.log(`Static UI preview running at http://${host}:${port}`);
});
