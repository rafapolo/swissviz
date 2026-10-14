// Tests the timeline clock (screen time <-> day) that lives in index.html between
// "// <tl-clock>" and "// </tl-clock>". Run: bun scripts/test_tl_clock.mjs
import { readFileSync } from "node:fs";
import { strict as assert } from "node:assert";

const html = readFileSync(new URL("../index.html", import.meta.url), "utf8");
const block = html.match(/\/\/ <tl-clock>[^\n]*\n([\s\S]*?)\/\/ <\/tl-clock>/);
assert.ok(block, "index.html has no // <tl-clock> block");
const { tlBuildClock, tlInterp, tlStepDay, tlSnapDay, tlMonthOf, tlMonthStart, tlScreenTimes, tlStats, TL_SPLIT_DAY, TL_DAYS_PER_SEC } =
    new Function("TL_NONE", "TL_FAR",
        `${block[1]}; return { tlBuildClock, tlInterp, tlStepDay, tlSnapDay, tlMonthOf, tlMonthStart, tlScreenTimes, tlStats, TL_SPLIT_DAY, TL_DAYS_PER_SEC };`)(65535, 1e9);

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
test("arrow keys step one year before 2018, one month after", () => {
    assert.equal(tlStepDay(DAY(1892, 6, 1), 1), DAY(1893));
    assert.equal(tlStepDay(DAY(1893), 1), DAY(1894));
    assert.equal(tlStepDay(DAY(1893), -1), DAY(1892));
    assert.equal(tlStepDay(DAY(1892, 6, 1), -1), DAY(1892));
    assert.equal(tlStepDay(DAY(2017, 3, 1), 1), DAY(2018));
    assert.equal(tlStepDay(DAY(2018), 1), DAY(2018, 2));
    assert.equal(tlStepDay(DAY(2018, 12, 20), 1), DAY(2019));
    assert.equal(tlStepDay(DAY(2018, 3, 15), -1), DAY(2018, 3));
    assert.equal(tlStepDay(DAY(2018, 3), -1), DAY(2018, 2));
    assert.equal(tlStepDay(DAY(2018), -1), DAY(2017));
});
test("from 2018 the slider lands on the 1st of a month; before, anywhere", () => {
    assert.equal(tlSnapDay(DAY(2020, 5, 17)), DAY(2020, 5));
    assert.equal(tlSnapDay(DAY(2018)), DAY(2018));
    assert.equal(tlSnapDay(DAY(1950, 5, 17)), DAY(1950, 5, 17));
    assert.equal(tlMonthStart(tlMonthOf(DAY(2024, 2, 29))), DAY(2024, 2));
    assert.equal(tlMonthOf(DAY(1884, 1, 10)), 12);
});
test("lamps get their opening and closing in screen seconds, so a flash never depends on the speed", () => {
    const days = Uint16Array.from([DAY(1890), DAY(1950, 7, 1), DAY(2019, 3, 1), 65535]);
    const out = tlScreenTimes(days, c);
    assert.ok(out instanceof Float32Array);
    for (let i = 0; i < 3; i++) assert.ok(Math.abs(out[i] - timeAt(days[i])) < 1e-3, `lamp ${i}`);
    assert.equal(out[3], 1e9);   // no date: never
    assert.ok(out[0] < out[1] && out[1] < out[2]);
});
test("counters count only the lamps kept by the legend", () => {
    const o = Uint16Array.from([3, 3, 10, 65535]);
    const cl = Uint16Array.from([65535, 12, 65535, 8]);
    const all = tlStats(o, cl, 14, null);
    assert.equal(all.cumOpen[15], 3);          // opened by day 14
    assert.equal(all.cumClose[15], 2);
    assert.equal(all.cumOpen[4], 2);           // opened by day 3
    assert.equal(all.monthOpen[0], 3);         // January 1883
    assert.equal(all.monthClose[0], 2);
    const some = tlStats(o, cl, 14, Uint8Array.from([0, 1, 1, 0]));
    assert.equal(some.cumOpen[15], 2);
    assert.equal(some.cumClose[15], 1);
    assert.equal(some.monthOpen[0], 2);
});
process.exit(failures ? 1 : 0);
