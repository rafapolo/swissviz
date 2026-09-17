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

// ── 1b. Guard the parse worker ───────────────────────────────────────────────
// index.html builds its parse worker by stringifying real top-level functions,
// so terser must not rename them or the names they reference — the worker
// redeclares each one verbatim. This only breaks in the minified build (dev
// serves the raw file), so assert it here rather than discovering it in prod.
const WORKER_FNS = ["hexToRgb", "getColor", "getLabel", "parseCantonData"];
const WORKER_REFS = ["T_DECODE", "colorAssignments", "_colorCache", "_labelCache"];

const renamed = [
    // A preserved declaration still reads `function getColor(`; a mangled one does not.
    ...WORKER_FNS.filter(n => !new RegExp(`function\\s+${n}\\s*\\(`).test(minified)),
    // These appear once as literal text inside the worker template even when
    // mangled, so a surviving top-level name means 2+ occurrences.
    ...WORKER_REFS.filter(n => minified.split(n).length - 1 < 2),
];

if (renamed.length) {
    throw new Error(
        `Minification renamed parse-worker names: ${renamed.join(", ")}.\n` +
        `The worker would throw ReferenceError at runtime. Keep minifyJS.mangle.toplevel falsy.`
    );
}

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
