// ===== MapReduce engine (pure logic, no DOM) =====
const STOP_WORDS = new Set([
  "the","a","an","and","or","but","is","are","was","were","be","been","to","of","in","on","at",
  "for","with","by","as","it","this","that","these","those","from","has","have","had","so","than",
  "then","if","do","does","not","no","we","you","they","he","she","i","my","our","their","its"
]);

function tokenize(text, removeStop) {
  const raw = text.toLowerCase().match(/[\p{L}\p{N}']+/gu) || [];
  const words = raw.map(w => w.replace(/^'+|'+$/g, "")).filter(Boolean);
  return removeStop ? words.filter(w => !STOP_WORDS.has(w)) : words;
}

// Simple deterministic string hash (djb2) used by the partitioner
function hashWord(w) {
  let h = 5381;
  for (let i = 0; i < w.length; i++) {
    h = ((h << 5) + h + w.charCodeAt(i)) >>> 0;
  }
  return h;
}

// Divide the words into m splits of (almost) equal size, like HDFS blocks
function splitWords(words, m) {
  const count = Math.min(m, words.length);
  const base = Math.floor(words.length / count);
  const extra = words.length % count;
  const splits = [];
  let idx = 0;
  for (let i = 0; i < count; i++) {
    const len = base + (i < extra ? 1 : 0);
    splits.push(words.slice(idx, idx + len));
    idx += len;
  }
  return splits;
}

function computeJob(words, mapperCount, reducerCount, useCombiner) {
  // 1. Split
  const splits = splitWords(words, mapperCount);

  // 2. Map: emit (word, 1)
  const mapOut = splits.map(sp => sp.map(w => [w, 1]));
  const rawPairs = mapOut.reduce((s, p) => s + p.length, 0);

  // 3. Combiner (optional): local aggregation inside each mapper
  const combined = mapOut.map(pairs => {
    const local = new Map();
    pairs.forEach(([w, v]) => local.set(w, (local.get(w) || 0) + v));
    return [...local.entries()];
  });
  const emitted = useCombiner ? combined : mapOut;
  const shuffleRecords = emitted.reduce((s, p) => s + p.length, 0);

  // 4. Shuffle, sort and partition: hash(word) mod reducers
  const partitions = Array.from({ length: reducerCount }, () => new Map());
  emitted.forEach(pairs => {
    pairs.forEach(([w, v]) => {
      const part = partitions[hashWord(w) % reducerCount];
      if (!part.has(w)) part.set(w, []);
      part.get(w).push(v);
    });
  });
  const grouped = partitions.map(part =>
    [...part.entries()].sort((a, b) => a[0].localeCompare(b[0]))
  );

  // 5. Reduce: add up the values of every key
  const reduced = grouped.map(list =>
    list.map(([w, vals]) => [w, vals.reduce((s, v) => s + v, 0)])
  );

  // 6. Final result
  const final = reduced.flat().sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));

  return { splits, mapOut, combined, rawPairs, shuffleRecords, grouped, reduced, final };
}

// ===== UI =====
const MAX_SHOWN = 12;      // chips shown per row
const MAX_WORDS = 20000;   // safety limit

const $ = id => document.getElementById(id);
const textInput = $("textInput");
const messageEl = $("message");
const outputEl = $("output");
const stagesEl = $("stages");

let lastResults = [];

const SAMPLE =
  "Big data needs big tools. Hadoop stores big data in HDFS and processes it with MapReduce.\n" +
  "Spark processes data fast because Spark keeps data in memory.\n" +
  "Hadoop and Spark work together to handle big data at scale.";

function el(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}

function chipsRow(label, items) {
  const row = el("div", "row");
  row.appendChild(el("div", "row-label", label));
  const wrap = el("div", "chips");
  items.slice(0, MAX_SHOWN).forEach(t => wrap.appendChild(el("span", "chip", t)));
  if (items.length > MAX_SHOWN) {
    wrap.appendChild(el("span", "chip more", "+" + (items.length - MAX_SHOWN) + " more"));
  }
  row.appendChild(wrap);
  return row;
}

function makeStage(num, title, desc, delay) {
  const card = el("div", "stage");
  card.style.animationDelay = delay + "s";
  const head = el("div", "stage-head");
  head.appendChild(el("div", "badge", num));
  head.appendChild(el("h3", "", title));
  card.appendChild(head);
  card.appendChild(el("p", "desc", desc));
  return card;
}

function valuesText(vals) {
  const shown = vals.slice(0, 6).join(", ");
  return "[" + shown + (vals.length > 6 ? ", ..." : "") + "]";
}

function render(job, opts) {
  stagesEl.innerHTML = "";
  let n = 0;
  let delay = 0;
  const add = (title, desc) => {
    n++;
    const card = makeStage(n, title, desc, delay);
    delay += 0.25;
    stagesEl.appendChild(card);
    return card;
  };

  // Input split
  const s1 = add("Input Split",
    "The input is divided into " + job.splits.length + " splits of roughly equal size, like HDFS blocks. Each split goes to one mapper.");
  job.splits.forEach((sp, i) => {
    const row = el("div", "row");
    row.appendChild(el("div", "row-label", "Split " + (i + 1) + " · " + sp.length + " words"));
    const preview = sp.slice(0, 25).join(" ") + (sp.length > 25 ? " ..." : "");
    row.appendChild(el("div", "split-text", preview));
    s1.appendChild(row);
  });

  // Map
  const s2 = add("Map Phase",
    "Each mapper reads its split and emits a (word, 1) pair for every word.");
  job.mapOut.forEach((pairs, i) => {
    s2.appendChild(chipsRow("Mapper " + (i + 1) + " output · " + pairs.length + " pairs",
      pairs.map(p => "(" + p[0] + ", " + p[1] + ")")));
  });

  // Combiner (optional)
  if (opts.useCombiner) {
    const s3 = add("Combiner",
      "A combiner adds up counts inside each mapper before sending data over the network, so fewer records are shuffled.");
    job.combined.forEach((pairs, i) => {
      s3.appendChild(chipsRow("Mapper " + (i + 1) + " combined output · " + pairs.length + " pairs",
        pairs.map(p => "(" + p[0] + ", " + p[1] + ")")));
    });
    const saved = job.rawPairs - job.shuffleRecords;
    const pct = job.rawPairs ? Math.round(saved / job.rawPairs * 100) : 0;
    s3.appendChild(el("p", "note",
      "Shuffle records reduced from " + job.rawPairs + " to " + job.shuffleRecords + " (" + pct + "% less data to move)."));
  }

  // Shuffle, sort and partition
  const s4 = add("Shuffle, Sort & Partition",
    "Pairs are partitioned using hash(word) mod " + opts.reducerCount +
    ", so every occurrence of a word reaches the same reducer. Keys are sorted inside each reducer.");
  job.grouped.forEach((list, i) => {
    s4.appendChild(chipsRow("Reducer " + (i + 1) + " receives " + list.length + " keys",
      list.map(([w, vals]) => w + " → " + valuesText(vals))));
  });

  // Reduce
  const s5 = add("Reduce Phase",
    "Each reducer adds up the values for every key it received to get the final count.");
  job.reduced.forEach((list, i) => {
    const sorted = [...list].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]));
    s5.appendChild(chipsRow("Reducer " + (i + 1) + " output", sorted.map(([w, c]) => w + ": " + c)));
  });

  // Final result
  const s6 = add("Final Result", "Top words by frequency across all reducers.");
  const top = job.final.slice(0, 10);
  const max = top[0][1];
  top.forEach(([w, c]) => {
    const row = el("div", "bar-row");
    row.appendChild(el("span", "bar-word", w));
    const track = el("div", "bar-track");
    const fill = el("div", "bar-fill");
    fill.style.width = (c / max * 100) + "%";
    track.appendChild(fill);
    row.appendChild(track);
    row.appendChild(el("span", "bar-count", c));
    s6.appendChild(row);
  });

  const dl = el("button", "secondary", "⬇ Download full result (CSV)");
  dl.addEventListener("click", downloadCSV);
  s6.appendChild(dl);
}

function run() {
  messageEl.textContent = "";
  const text = textInput.value.trim();

  if (!text) {
    messageEl.textContent = "Please enter some text or upload a .txt file first.";
    outputEl.classList.add("hidden");
    return;
  }

  const removeStop = $("removeStop").checked;
  const useCombiner = $("useCombiner").checked;
  const mapperCount = parseInt($("mapperCount").value, 10);
  const reducerCount = parseInt($("reducerCount").value, 10);

  let words = tokenize(text, removeStop);
  if (words.length === 0) {
    messageEl.textContent = removeStop
      ? "No words left after removing stop words. Try different text."
      : "No words found. Please enter some text.";
    outputEl.classList.add("hidden");
    return;
  }
  if (words.length > MAX_WORDS) {
    words = words.slice(0, MAX_WORDS);
    messageEl.textContent = "Only the first " + MAX_WORDS + " words are used.";
  }

  const job = computeJob(words, mapperCount, reducerCount, useCombiner);
  lastResults = job.final;

  $("statMappers").textContent = job.splits.length;
  $("statReducers").textContent = reducerCount;
  $("statWords").textContent = words.length;
  $("statUnique").textContent = job.final.length;
  $("statShuffle").textContent = job.shuffleRecords;

  render(job, { useCombiner, reducerCount });
  outputEl.classList.remove("hidden");
  outputEl.scrollIntoView({ behavior: "smooth", block: "start" });
}

function downloadCSV() {
  const lines = ["word,count"].concat(lastResults.map(([w, c]) => w + "," + c));
  const blob = new Blob([lines.join("\n")], { type: "text/csv" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "word_count_result.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

// ===== Events =====
$("runBtn").addEventListener("click", run);

$("sampleBtn").addEventListener("click", () => {
  textInput.value = SAMPLE;
  run();
});

$("clearBtn").addEventListener("click", () => {
  textInput.value = "";
  messageEl.textContent = "";
  outputEl.classList.add("hidden");
  stagesEl.innerHTML = "";
  lastResults = [];
});

$("fileInput").addEventListener("change", () => {
  const input = $("fileInput");
  const file = input.files[0];
  if (!file) return;
  if (file.size > 1000000) {
    messageEl.textContent = "File is too large. Please use a .txt file under 1 MB.";
    input.value = "";
    return;
  }
  const reader = new FileReader();
  reader.onload = () => {
    textInput.value = reader.result;
    messageEl.textContent = "Loaded " + file.name + ". Click Run MapReduce.";
  };
  reader.readAsText(file);
  input.value = "";
});