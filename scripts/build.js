#!/usr/bin/env bun
// Build script: minify index.html (inline CSS + JS) → dist/
// Also copies sw.js and all data/ assets to dist/

import { minify } from "html-minifier-terser";
import { copyFileSync, cpSync, mkdirSync, readFileSync, writeFileSync, statSync } from "fs";
import { join } from "path";

const ROOT = new URL("..", import.meta.url).pathname;
const DIST = join(ROOT, "dist");

mkdirSync(DIST, { recursive: true });

// ── 1. Minify index.html ─────────────────────────────────────────────────────
const src = readFileSync(join(ROOT, "index.html"), "utf-8");

const minified = await minify(src, {
    collapseWhitespace: true,
    removeComments: true,
    removeRedundantAttributes: true,
    removeScriptTypeAttributes: true,
    removeStyleLinkTypeAttributes: true,
    useShortDoctype: true,
    minifyCSS: true,
    minifyJS: {
        compress: {
            drop_console: false,
            passes: 2,
        },
        mangle: true,
        format: {
            comments: false,
        },
    },
    sortAttributes: true,
    sortClassName: false,
});

writeFileSync(join(DIST, "index.html"), minified);

// ── 2. Copy service worker (already small; copy verbatim so SW scope is root) ─
copyFileSync(join(ROOT, "sw.js"), join(DIST, "sw.js"));

// ── 3. Copy static assets ─────────────────────────────────────────────────────
cpSync(join(ROOT, "data"), join(DIST, "data"), { recursive: true });
copyFileSync(join(ROOT, "mini.jpg"), join(DIST, "mini.jpg"));
copyFileSync(join(ROOT, "sample2.jpg"), join(DIST, "sample2.jpg"));

// ── 4. Report sizes ───────────────────────────────────────────────────────────
const srcSize  = statSync(join(ROOT, "index.html")).size;
const distSize = statSync(join(DIST, "index.html")).size;
const swSize   = statSync(join(DIST, "sw.js")).size;
const saved    = srcSize - distSize;
const pct      = ((saved / srcSize) * 100).toFixed(1);

console.log(`index.html: ${(srcSize / 1024).toFixed(1)} KB → ${(distSize / 1024).toFixed(1)} KB  (−${pct}%, saved ${(saved / 1024).toFixed(1)} KB)`);
console.log(`sw.js:      ${(swSize / 1024).toFixed(1)} KB  (unchanged)`);
console.log(`dist/ ready at: ${DIST}`);
