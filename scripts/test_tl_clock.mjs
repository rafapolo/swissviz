// Tests the timeline clock (screen time <-> day) that lives in index.html between
// "// <tl-clock>" and "// </tl-clock>". Run: bun scripts/test_tl_clock.mjs
import { readFileSync } from "node:fs";
import { strict as assert } from "node:assert";

const html = readFileSync(new URL("../index.html", import.meta.url), "utf8");
const block = html.match(/\/\/ <tl-clock>[^\n]*\n([\s\S]*?)\/\/ <\/tl-clock>/);
assert.ok(block, "index.html has no // <tl-clock> block");
const { tlBuildClock, tlInterp, tlStepDay, TL_SPLIT_DAY, TL_DAYS_PER_SEC } =
    new Function(`${block[1]}; return { tlBuildClock, tlInterp, tlStepDay, TL_SPLIT_DAY, TL_DAYS_PER_SEC };`)();

const DAY = (y, m = 1, d = 1) => Math.round((Date.UTC(y, m - 1, d) - Date.UTC(1883, 0, 1)) / 864e5);
const lastDay = DAY(2026, 10, 12);
// openings grow with time, like the register: a few per year in 1883, thousands in the 2010s
const cumOpen = new Uint32Array(lastDay + 2);
for (let d = 0; d <= lastDay; d++) {
    const y = 1883 + d / 365.25;
    cumOpen[d + 1] = cumOpen[d] + (y < 2018 ? Math.round(Math.exp((y - 1883) / 14) / 30) : 150);
}
const firstDay = 0;
const c = tlBuildClock(cumOpen, firstDay, lastDay);
const dayAt = (t) => tlInterp(c.clockT, c.clockDay, t);
const timeAt = (d) => tlInterp(c.clockDay, c.clockT, d);
let failures = 0;
const test = (name, fn) => {
    try { fn(); console.log(`ok   ${name}`); } catch (e) { failures++; console.log(`FAIL ${name}: ${e.message}`); }
};

test("starts at the first day and ends at the last", () => {
    assert.equal(dayAt(0), firstDay);
    assert.ok(Math.abs(dayAt(c.tMax) - lastDay) < 1);
});
test("1883-2018 takes about 45 s at 1x", () => {
    const t = timeAt(TL_SPLIT_DAY);
    assert.ok(t >= 40 && t <= 50, `took ${t.toFixed(1)} s`);
});
test("every year before 2018 lasts between 1/8 s and 2 s", () => {
    for (let y = 1883; y < 2018; y++) {
        const dur = timeAt(DAY(y + 1)) - timeAt(DAY(y));
        assert.ok(dur >= 0.125 - 1e-6 && dur <= 2 + 1e-6, `${y}: ${dur.toFixed(3)} s`);
    }
});
test("from 2018 on, the rate is a year in ~7 s", () => {
    const rate = (dayAt(timeAt(DAY(2020)) + 1) - DAY(2020));
    assert.ok(Math.abs(rate - TL_DAYS_PER_SEC) < 0.01, `rate ${rate}`);
});
test("time and day convert back and forth", () => {
    for (const d of [DAY(1890, 5, 3), DAY(1955), DAY(2017, 12, 31), DAY(2019, 3, 1)])
        assert.ok(Math.abs(dayAt(timeAt(d)) - d) < 1e-3, `day ${d}`);
});
test("a data set that starts in 2018 has no historical part", () => {
    const short = tlBuildClock(cumOpen, TL_SPLIT_DAY + 200, lastDay);
    assert.equal(short.clockDay[0], TL_SPLIT_DAY + 200);
    assert.ok(Math.abs(short.tMax - (lastDay - TL_SPLIT_DAY - 200) / TL_DAYS_PER_SEC) < 1e-6);
});
test("years with no opening still pass, at the slowest allowed speed or faster", () => {
    const empty = new Uint32Array(lastDay + 2);   // no openings at all before 2018
    for (let d = TL_SPLIT_DAY; d <= lastDay; d++) empty[d + 1] = empty[d] + 1;
    const e = tlBuildClock(empty, 0, lastDay);
    const t = tlInterp(e.clockDay, e.clockT, TL_SPLIT_DAY);
    assert.ok(Number.isFinite(t) && t > 0, `t ${t}`);
});
test("arrow keys step one year before 2018, one week after", () => {
    assert.equal(tlStepDay(DAY(1892, 6, 1), 1), DAY(1893));
    assert.equal(tlStepDay(DAY(1893), 1), DAY(1894));
    assert.equal(tlStepDay(DAY(1893), -1), DAY(1892));
    assert.equal(tlStepDay(DAY(1892, 6, 1), -1), DAY(1892));
    assert.equal(tlStepDay(DAY(2017, 3, 1), 1), DAY(2018));
    assert.equal(tlStepDay(DAY(2018), 1), DAY(2018) + 7);
    assert.equal(tlStepDay(DAY(2018) + 3, -1), DAY(2018));
    assert.equal(tlStepDay(DAY(2018), -1), DAY(2017));
});
process.exit(failures ? 1 : 0);
