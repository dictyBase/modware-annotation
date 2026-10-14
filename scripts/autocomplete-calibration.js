'use strict';

// Calibration harness for the annotation autocomplete ArangoSearch feature.
//
// Run with arangosh against a disposable ArangoDB database:
//
//   docker exec -i arangodb arangosh \
//     --server.endpoint tcp://127.0.0.1:8529 \
//     --server.username root --server.password "$ARANGO_PASS" \
//     --javascript.execute /scripts/autocomplete-calibration.js
//
// It creates a disposable database, the three analyzers, the
// annotation_autocomplete view, ~1,000 annotations, then runs 12 probes,
// an EXPLAIN node-count check, a threshold sweep, and a 20-run latency
// measurement. Results are printed as JSON.
//
// Environment:
//   CALIB_KEEP=1   keep the disposable database after the run

var CALIB = {
  dbName: 'annotation_calib',
  annotation: 'annotation_calib',
  cv: 'cv_calib',
  cvterm: 'cvterm_calib',
  edge: 'annotation_cvterm_calib',
  graph: 'annotation_tag_calib',
  view: 'annotation_autocomplete',
  norm: 'annotation_autocomplete_norm',
  delim: 'annotation_autocomplete_delim',
  ngram: 'annotation_autocomplete_ngram',
  ontology: 'dicty_annotation',
  synonym: 'synonym',
  genotype: 'genotype',
  description: 'description',
  ngramThreshold: 0.3,
  ngramMin: 2,
  ngramMax: 3,
  fixtureCount: 1000,
};

function format(template) {
  var args = Array.prototype.slice.call(arguments, 1);
  var i = 0;
  return template.replace(/%[sq]/g, function () {
    return String(args[i++]);
  });
}

function sleepSeconds(seconds) {
  try {
    require('internal').sleep(seconds);
  } catch (e) {
    var deadline = Date.now() + seconds * 1000;
    while (Date.now() < deadline) {
      // busy wait fallback
    }
  }
}

var results = {
  arangoVersion: db._version().version,
  database: CALIB.dbName,
  probes: [],
  thresholdSweep: [],
  explain: {},
  latency: {},
  warnings: [],
};

function log(msg) {
  print('[calib] ' + msg);
}

// ---------------------------------------------------------------- database

function dropDisposableDatabase() {
  db._useDatabase('_system');
  var databases = db._databases();
  if (databases.indexOf(CALIB.dbName) !== -1) {
    db._dropDatabase(CALIB.dbName);
  }
}

function createDisposableDatabase() {
  db._useDatabase('_system');
  db._createDatabase(CALIB.dbName);
  db._useDatabase(CALIB.dbName);
}

// --------------------------------------------------------------- analyzers

function createAnalyzers() {
  var analyzers = require('@arangodb/analyzers');
  var existing = {};
  analyzers.toArray().forEach(function (a) {
    existing[String(a.name()).replace(/^[^:]*::/, '')] = true;
  });

  if (!existing[CALIB.norm]) {
    analyzers.save(
      CALIB.norm,
      'norm',
      {
        locale: 'en.utf-8',
        case: 'lower',
        accent: false,
      },
      []
    );
  }

  if (!existing[CALIB.delim]) {
    analyzers.save(
      CALIB.delim,
      'pipeline',
      {
        pipeline: [
          { type: 'delimiter', properties: { delimiter: ',' } },
          { type: 'delimiter', properties: { delimiter: ' ' } },
          {
            type: 'norm',
            properties: { locale: 'en.utf-8', case: 'lower', accent: false },
          },
        ],
      },
      ['frequency', 'norm', 'position']
    );
  }

  if (!existing[CALIB.ngram]) {
    analyzers.save(
      CALIB.ngram,
      'pipeline',
      {
        pipeline: [
          {
            type: 'norm',
            properties: { locale: 'en.utf-8', case: 'lower', accent: false },
          },
          {
            type: 'ngram',
            properties: {
              min: CALIB.ngramMin,
              max: CALIB.ngramMax,
              preserveOriginal: true,
              streamType: 'utf8',
            },
          },
        ],
      },
      ['frequency', 'norm', 'position']
    );
  }
}

// -------------------------------------------------------------- collections

function createCollections() {
  db._create(CALIB.annotation, { waitForSync: false });
  db._create(CALIB.cv, { waitForSync: false });
  db._create(CALIB.cvterm, { waitForSync: false });
  db._createEdgeCollection(CALIB.edge, { waitForSync: false });

  require('@arangodb/general-graph')._create(CALIB.graph, [
    {
      collection: CALIB.edge,
      from: [CALIB.annotation],
      to: [CALIB.cvterm],
    },
  ]);

  var cv = db._collection(CALIB.cv);
  var cvDoc = cv.insert({
    metadata: { namespace: CALIB.ontology },
  });

  var terms = {};
  [CALIB.synonym, CALIB.genotype, CALIB.description].forEach(function (label) {
    terms[label] = db._collection(CALIB.cvterm).insert({
      label: label,
      deprecated: false,
      graph_id: cvDoc._id,
    });
  });

  return terms;
}

// ------------------------------------------------------------------- view

function createView() {
  var fields = {};
  fields.value = {
    analyzers: [CALIB.norm, CALIB.delim, CALIB.ngram],
  };
  fields.entry_id = { analyzers: [CALIB.norm] };
  fields.is_obsolete = { analyzers: ['identity'] };

  var links = {};
  links[CALIB.annotation] = { fields: fields };

  db._createView(CALIB.view, 'arangosearch', {
    commitIntervalMsec: 100,
    links: links,
  });
}

// ---------------------------------------------------------------- fixtures

function makeRandom(seed) {
  var s = seed >>> 0;
  return function () {
    s = (s + 0x6d2b79f5) >>> 0;
    var t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function insertAnnotation(terms, entryId, tag, value, opts) {
  var options = opts || {};
  var ann = db._collection(CALIB.annotation).insert({
    value: value,
    editable_value: value,
    created_by: 'siddbasu@gmail.com',
    entry_id: entryId,
    rank: options.rank || 0,
    is_obsolete: options.isObsolete === true,
    version: 1,
    created_at: new Date().toISOString(),
  });

  db._collection(CALIB.edge).insert({
    _from: ann._id,
    _to: terms[tag]._id,
  });

  return ann;
}

function insertFixtures(terms) {
  // Probe 1/5/6: whole-value synonym prefix, case and accent folding, typo.
  insertAnnotation(terms, 'DBS0236125', CALIB.synonym, 'gammaS13');

  // Probe 2/3/4: multi-allele genotype, whole value and interior alleles.
  insertAnnotation(
    terms,
    'DBS0236126',
    CALIB.genotype,
    'axeA1, axeB1, sadA-[sadA-KO]'
  );

  // Probe 7/8: obsolete version retired by copy-on-write edit.
  insertAnnotation(terms, 'DBS0236128', CALIB.synonym, 'oldAlias', {
    isObsolete: true,
  });
  insertAnnotation(terms, 'DBS0236128', CALIB.synonym, 'newAlias', { rank: 1 });

  // Probe 9: non-strain gene entries that share the probe value.
  for (var g = 0; g < 10; g++) {
    insertAnnotation(
      terms,
      'DDB_G028642' + g,
      CALIB.synonym,
      'xyloseMutant'
    );
  }

  // Probe 10: same value under a different tag plus one real strain synonym.
  for (var d = 0; d < 10; d++) {
    insertAnnotation(
      terms,
      'DBS023620' + d,
      CALIB.description,
      'xyloseMutant'
    );
  }
  insertAnnotation(terms, 'DBS0236999', CALIB.synonym, 'xyloseMutant');

  // Probe 11: multiple synonyms plus a genotype on one strain.
  insertAnnotation(terms, 'DBS0236129', CALIB.synonym, 'sadA-null', {
    rank: 0,
  });
  insertAnnotation(terms, 'DBS0236129', CALIB.synonym, 'sadA-ko', { rank: 1 });
  insertAnnotation(
    terms,
    'DBS0236129',
    CALIB.genotype,
    'sadA-[sadA-KO]'
  );

  // Probe 12: one prefix hit against six fuzzy near-hits.
  insertAnnotation(terms, 'DBS0237001', CALIB.synonym, 'alphaOne');
  insertAnnotation(terms, 'DBS0237002', CALIB.synonym, 'alpaOne');
  insertAnnotation(terms, 'DBS0237003', CALIB.synonym, 'alpnaOne');
  insertAnnotation(terms, 'DBS0237004', CALIB.synonym, 'alpha0ne');
  insertAnnotation(terms, 'DBS0237005', CALIB.synonym, 'alphaOn3');
  insertAnnotation(terms, 'DBS0237006', CALIB.synonym, 'alphaOn2');
  insertAnnotation(terms, 'DBS0237007', CALIB.synonym, 'alphaOnx');

  // Filler keeps the index realistic without matching any probe query.
  var rand = makeRandom(20261014);
  var fillerTags = [CALIB.synonym, CALIB.genotype, CALIB.description];
  for (var i = 0; i < CALIB.fixtureCount; i++) {
    var tag = fillerTags[Math.floor(rand() * fillerTags.length)];
    var entry =
      rand() < 0.5
        ? 'DBS02' + (300000 + Math.floor(rand() * 90000))
        : 'DDB_G0' + (300000 + Math.floor(rand() * 900000));
    var value =
      tag === CALIB.genotype
        ? 'zzfill' + i + 'A1, zzfill' + i + 'B2, zzfill' + i + 'C3'
        : 'zzfill' + i + ' ' + (rand() < 0.5 ? 'allele' : 'mutation');
    insertAnnotation(terms, entry, tag, value, {
      rank: Math.floor(rand() * 3),
    });
  }
}

// ------------------------------------------------------------- query shapes

var BRANCH = [
  'LET %s = (',
  '  FOR d IN ' + CALIB.view,
  '    SEARCH d.is_obsolete == false',
  '      AND ANALYZER(STARTS_WITH(d.entry_id, "dbs"), "%s")',
  '      AND %s',
  '    LET has_tag = FIRST(',
  '      FOR cvt, e IN 1..1 OUTBOUND d GRAPH @anno_cvterm_graph',
  '        FILTER e._to == @%s AND cvt.deprecated == false',
  '        RETURN true',
  '    )',
  '    FILTER has_tag == true',
  '    SORT BM25(d) DESC, d.entry_id ASC, d._key ASC',
  '    LIMIT @limit',
  '    RETURN { id: d.entry_id, f: %s, v: NOT_NULL(d.value, ""), s: %s }',
  ')',
].join('\n');

var MERGE = [
  'LET hits = FLATTEN([%s])',
  'LET best = (',
  '  FOR x IN hits',
  '    COLLECT eid = x.id INTO grp = x',
  '    LET top = FIRST(FOR m IN grp SORT m.s DESC, m.f ASC, m.v ASC RETURN m)',
  '    RETURN top',
  ')',
  'FOR x IN best',
  '  SORT x.s DESC, x.id ASC',
  '  LIMIT @limit',
  '  RETURN { id: x.id, f: x.f, v: x.v, s: x.s }',
].join('\n');

function synonymPrefixExpr() {
  return (
    'ANALYZER(STARTS_WITH(d.value, @q), "' + CALIB.norm + '")'
  );
}

function genotypePrefixExpr() {
  return (
    '(ANALYZER(STARTS_WITH(d.value, @q), "' +
    CALIB.norm +
    '") OR ANALYZER(STARTS_WITH(d.value, @q), "' +
    CALIB.delim +
    '"))'
  );
}

function fuzzyExpr() {
  return 'NGRAM_MATCH(d.value, @q, @th, "' + CALIB.ngram + '")';
}

function branch(name, expr, termBind, field, score) {
  return format(
    BRANCH,
    name,
    CALIB.norm,
    expr,
    termBind,
    JSON.stringify(field),
    score
  );
}

function buildQuery() {
  var branches = [
    branch('p_synonym', synonymPrefixExpr(), 'synonym_term_id', CALIB.synonym, '1000 + BM25(d)'),
    branch('p_genotype', genotypePrefixExpr(), 'genotype_term_id', CALIB.genotype, '1000 + BM25(d)'),
    branch('n_synonym', fuzzyExpr(), 'synonym_term_id', CALIB.synonym, 'BM25(d)'),
    branch('n_genotype', fuzzyExpr(), 'genotype_term_id', CALIB.genotype, 'BM25(d)'),
  ];
  var merge = format(MERGE, branches.map(function (b) {
    return b.split('\n')[0].split(' ')[1];
  }).join(', '));
  return branches.join('\n') + '\n' + merge;
}

function ngramOnlyQuery(threshold) {
  return (
    'FOR d IN ' +
    CALIB.view +
    ' SEARCH NGRAM_MATCH(d.value, @q, @th, "' +
    CALIB.ngram +
    '")' +
    ' RETURN MERGE({ id: d.entry_id, v: d.value }, { s: BM25(d) })'
  );
}

// ------------------------------------------------------------------- probes

function runQuery(query, termIds, text, limit, threshold) {
  return db
    ._query(query, {
      q: text,
      th: threshold === undefined ? CALIB.ngramThreshold : threshold,
      limit: limit === undefined ? 5 : limit,
      anno_cvterm_graph: CALIB.graph,
      synonym_term_id: termIds[CALIB.synonym]._id,
      genotype_term_id: termIds[CALIB.genotype]._id,
    })
    .toArray();
}

function waitForCommit(probe, timeoutMs) {
  var deadline = Date.now() + (timeoutMs || 30000);
  var latest = [];
  while (Date.now() < deadline) {
    latest = probe();
    if (latest && latest.length > 0) {
      return latest;
    }
    sleepSeconds(0.25);
  }
  return latest;
}

function probe(name, fn) {
  var entry = { name: name };
  try {
    entry.result = fn();
    entry.ok = true;
  } catch (e) {
    entry.ok = false;
    entry.error = String(e && e.message ? e.message : e);
  }
  results.probes.push(entry);
  log('probe ' + name + ' -> ' + JSON.stringify(entry));
}

function main() {
  dropDisposableDatabase();
  createDisposableDatabase();
  createAnalyzers();
  var terms = createCollections();
  createView();
  insertFixtures(terms);

  var query = buildQuery();
  results.query = query;

  // Wait for the first probe target to appear in the view.
  waitForCommit(function () {
    return runQuery(query, terms, 'gammas', 5);
  });
  waitForCommit(function () {
    return runQuery(query, terms, 'alpha', 5);
  });

  probe('1_norm_synonym_prefix', function () {
    var rows = runQuery(query, terms, 'gammas', 5);
    return {
      rows: rows,
      pass: rows.some(function (r) {
        return r.id === 'DBS0236125' && r.f === 'synonym' && r.v === 'gammaS13' && r.s >= 1000;
      }),
    };
  });

  probe('2_norm_genotype_whole_prefix', function () {
    var rows = runQuery(query, terms, 'axea', 5);
    return {
      rows: rows,
      pass: rows.some(function (r) {
        return r.id === 'DBS0236126' && r.f === 'genotype' && r.s >= 1000;
      }),
    };
  });

  probe('3_delim_genotype_interior_axeb1', function () {
    var rows = runQuery(query, terms, 'axeb1', 5);
    return {
      rows: rows,
      pass: rows.some(function (r) {
        return r.id === 'DBS0236126' && r.f === 'genotype' && r.s >= 1000;
      }),
    };
  });

  probe('4_delim_genotype_interior_sada', function () {
    var rows = runQuery(query, terms, 'sada', 10);
    return {
      rows: rows,
      pass: rows.some(function (r) {
        return r.id === 'DBS0236126' && r.f === 'genotype' && r.s >= 1000;
      }),
    };
  });

  probe('5_case_and_accent_folding', function () {
    var rows = runQuery(query, terms, 'g\u00e1mmas13', 5);
    return {
      rows: rows,
      pass: rows.some(function (r) {
        return r.id === 'DBS0236125';
      }),
    };
  });

  probe('6_ngram_typo_fuzzy', function () {
    var rows = runQuery(query, terms, 'gammas14', 5);
    return {
      rows: rows,
      pass: rows.some(function (r) {
        return r.id === 'DBS0236125' && r.s < 1000;
      }),
    };
  });

  probe('7_obsolete_value_returns_nothing', function () {
    var rows = runQuery(query, terms, 'oldalias', 5);
    return {
      rows: rows,
      pass: rows.length === 0,
    };
  });

  probe('8_updated_value_matches', function () {
    var rows = runQuery(query, terms, 'newalias', 5);
    return {
      rows: rows,
      pass: rows.some(function (r) {
        return r.id === 'DBS0236128' && r.v === 'newAlias';
      }),
    };
  });

  probe('9_non_strain_entries_excluded', function () {
    var rows = runQuery(query, terms, 'xylose', 50);
    return {
      rows: rows,
      pass: rows.every(function (r) {
        return r.id.indexOf('DDB_G') !== 0;
      }),
    };
  });

  probe('10_other_tags_excluded_under_small_limit', function () {
    var rows = runQuery(query, terms, 'xylose', 1);
    return {
      rows: rows,
      pass: rows.length === 1 && rows[0].id === 'DBS0236999',
    };
  });

  probe('11_intra_strain_dedup', function () {
    var rows = runQuery(query, terms, 'sada', 50);
    var mine = rows.filter(function (r) {
      return r.id === 'DBS0236129';
    });
    return {
      rows: rows,
      pass: mine.length === 1,
    };
  });

  probe('12_prefix_ranked_ahead_under_small_limit', function () {
    var rows = runQuery(query, terms, 'alpha', 3);
    return {
      rows: rows,
      pass: rows.length > 0 && rows[0].id === 'DBS0237001',
    };
  });

  // ------------------------------------------------------ threshold sweep

  waitForCommit(function () {
    return db
      ._query(ngramOnlyQuery(), {
        q: 'gammas14',
        th: CALIB.ngramThreshold,
      })
      .toArray();
  });

  for (var th = 10; th <= 90; th += 10) {
    var t = th / 100;
    var matched = db
      ._query(ngramOnlyQuery(), { q: 'gammas14', th: t })
      .toArray()
      .filter(function (r) {
        return r.id === 'DBS0236125';
      });
    results.thresholdSweep.push({
      threshold: t,
      matched: matched.length > 0,
      score: matched.length > 0 ? matched[0].s : null,
    });
  }

  // --------------------------------------------------------------- EXPLAIN

  var explained = db._connection.POST(
    '/_api/explain',
    JSON.stringify({
      query: query,
      bindVars: {
        q: 'gammas',
        th: CALIB.ngramThreshold,
        limit: 5,
        anno_cvterm_graph: CALIB.graph,
        synonym_term_id: terms[CALIB.synonym]._id,
        genotype_term_id: terms[CALIB.genotype]._id,
      },
    }),
    { 'content-type': 'application/json' }
  );
  var counts = {};
  (explained.plan ? explained.plan.nodes : []).forEach(function (node) {
    counts[node.type] = (counts[node.type] || 0) + 1;
  });
  results.explain = {
    nodes: counts,
    enumerateViewNode: counts.EnumerateViewNode || 0,
    enumerateCollectionNode: counts.EnumerateCollectionNode || 0,
    pass:
      (counts.EnumerateViewNode || 0) === 4 &&
      (counts.EnumerateCollectionNode || 0) === 0,
    fullCounts: explained.plan
      ? explained.plan.nodes.map(function (n) {
          return n.type;
        })
      : [],
  };

  // --------------------------------------------------------------- latency

  var timings = [];
  var lastRows = [];
  for (var run = 0; run < 20; run++) {
    var started = Date.now();
    lastRows = runQuery(query, terms, 'gammas', 5);
    timings.push(Date.now() - started);
  }
  timings.sort(function (a, b) {
    return a - b;
  });
  results.latency = {
    runs: timings,
    minMs: timings[0],
    medianMs: timings[Math.floor(timings.length / 2)],
    p95Ms: timings[Math.ceil(timings.length * 0.95) - 1],
    lastRowsMatched: lastRows.length,
    fixtureDocuments: db._collection(CALIB.annotation).count(),
  };

  if (!(process.env.CALIB_KEEP === '1')) {
    dropDisposableDatabase();
  }

  print(JSON.stringify(results, null, 2));

  var failures = results.probes
    .filter(function (p) {
      return !p.ok || !p.result || p.result.pass !== true;
    })
    .map(function (p) {
      return p.name;
    });
  if (!results.explain.pass) {
    failures.push('explain');
  }
  results.thresholdSweep.forEach(function (s) {
    if (s.threshold === CALIB.ngramThreshold && !s.matched) {
      failures.push('threshold_' + CALIB.ngramThreshold);
    }
  });
  if (failures.length > 0) {
    throw new Error('calibration failed: ' + failures.join(', '));
  }
  log('all calibration gates passed');
}

main();
