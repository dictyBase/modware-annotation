'use strict';

// Calibration harness for the annotation full search ArangoSearch feature.
//
// Run with arangosh against a disposable ArangoDB database:
//
//   docker exec -i arango311 arangosh \
//     --server.endpoint tcp://127.0.0.1:8529 \
//     --server.username root --server.password "$ARANGO_PASS" \
//     --javascript.execute /scripts/full-search-calibration.js
//
// It creates a disposable database, the three custom analyzers, the
// annotation_full_search view linking annotation and cvterm collections,
// ~1,000 annotations (including 6 ontologies and high-fan-out term),
// resolves targets with fullSearchTargetsQ, runs all 15 calibration probes,
// validates the EXPLAIN node counts (28 EnumerateViewNode, 0 EnumerateCollectionNode,
// 28 TraversalNode), and measures a 20-run latency baseline.
//
// Environment:
//   CALIB_KEEP=1   keep the disposable database after the run

var CALIB = {
  dbName: 'annotation_fullsearch_calib',
  annotation: 'annotation_calib',
  cv: 'cv_calib',
  cvterm: 'cvterm_calib',
  edge: 'annotation_cvterm_calib',
  graph: 'annotation_tag_calib',
  view: 'annotation_full_search',
  norm: 'annotation_search_norm',
  delim: 'annotation_search_delim',
  ngram: 'annotation_search_ngram',
  text: 'text_en',
  identity: 'identity',
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
  arangoVersion: db._version(),
  database: CALIB.dbName,
  targets: {},
  probes: [],
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
  var cvterm = db._collection(CALIB.cvterm);

  // 1. dicty_annotation
  var cvAnno = cv.insert({ metadata: { namespace: 'dicty_annotation' } });
  var terms = {};
  [
    'synonym',
    'genotype',
    'systematic name',
    'mutant type',
    'public note',
    'description',
  ].forEach(function (label) {
    terms['dicty_annotation:' + label] = cvterm.insert({
      label: label,
      deprecated: false,
      graph_id: cvAnno._id,
    });
  });

  // 2. Dd Mutagenesis Method
  var cvMut = cv.insert({ metadata: { namespace: 'Dd Mutagenesis Method' } });
  terms['Dd Mutagenesis Method:mutagenesis method'] = cvterm.insert({
    label: 'mutagenesis method',
    deprecated: false,
    graph_id: cvMut._id,
  });

  // 3. Dicty Phenotypes
  var cvPheno = cv.insert({ metadata: { namespace: 'Dicty Phenotypes' } });
  terms['phenotype:aberrant'] = cvterm.insert({
    label: 'aberrant fruiting body morphogenesis',
    deprecated: false,
    graph_id: cvPheno._id,
  });
  terms['phenotype:popular'] = cvterm.insert({
    label: 'popularpheno phenotype',
    deprecated: false,
    graph_id: cvPheno._id,
  });
  terms['phenotype:cordax'] = cvterm.insert({
    label: 'aberrant cordax development',
    deprecated: false,
    graph_id: cvPheno._id,
  });
  terms['phenotype:obsolete'] = cvterm.insert({
    label: 'obsoletephenoterm',
    deprecated: true,
    graph_id: cvPheno._id,
  });

  // 4. Dicty Environment
  var cvEnv = cv.insert({ metadata: { namespace: 'Dicty Environment' } });
  terms['environment:suspension'] = cvterm.insert({
    label: 'in suspension',
    deprecated: false,
    graph_id: cvEnv._id,
  });

  // 5. Dictyostelium Assay
  var cvAssay = cv.insert({ metadata: { namespace: 'Dictyostelium Assay' } });
  terms['assay:confocal'] = cvterm.insert({
    label: 'confocal microscopy',
    deprecated: false,
    graph_id: cvAssay._id,
  });

  // 6. dicty_strain_characteristics
  var cvChar = cv.insert({ metadata: { namespace: 'dicty_strain_characteristics' } });
  terms['characteristic:axenic'] = cvterm.insert({
    label: 'axenic',
    deprecated: false,
    graph_id: cvChar._id,
  });

  return terms;
}

// ------------------------------------------------------------------- view

function createView() {
  var links = {};
  links[CALIB.annotation] = {
    analyzers: [CALIB.identity],
    fields: {
      value: {
        analyzers: [CALIB.norm, CALIB.delim, CALIB.ngram, CALIB.text],
      },
      entry_id: { analyzers: [CALIB.norm] },
      is_obsolete: { analyzers: [CALIB.identity] },
    },
  };
  links[CALIB.cvterm] = {
    analyzers: [CALIB.identity],
    fields: {
      label: {
        analyzers: [CALIB.norm, CALIB.ngram, CALIB.text],
      },
      graph_id: { analyzers: [CALIB.identity] },
      deprecated: { analyzers: [CALIB.identity] },
    },
  };

  db._createView(CALIB.view, 'arangosearch', {
    commitIntervalMsec: 100,
    links: links,
  });
}

// ------------------------------------------------------------- target resolution

function resolveTargets() {
  var q = [
    'LET tag_rows = (',
    '  FOR cv IN @@cv_collection',
    '    FILTER cv.metadata.namespace IN @tag_ontologies',
    '    FOR cvt IN @@cvterm_collection',
    '      FILTER cvt.graph_id == cv._id',
    '        AND cvt.deprecated == false',
    '        AND cvt.label IN @tag_labels',
    '      RETURN {',
    '        k: CONCAT(cv.metadata.namespace, ":", cvt.label),',
    '        v: cvt._id',
    '      }',
    ')',
    'LET graph_rows = (',
    '  FOR cv IN @@cv_collection',
    '    FILTER cv.metadata.namespace IN @term_ontologies',
    '    RETURN {',
    '      k: cv.metadata.namespace,',
    '      v: cv._id',
    '    }',
    ')',
    'RETURN {',
    '  tags: ZIP(tag_rows[*].k, tag_rows[*].v),',
    '  graphs: ZIP(graph_rows[*].k, graph_rows[*].v)',
    '}',
  ].join('\n');

  var res = db
    ._query(q, {
      '@cv_collection': CALIB.cv,
      '@cvterm_collection': CALIB.cvterm,
      tag_ontologies: ['dicty_annotation', 'Dd Mutagenesis Method'],
      tag_labels: [
        'synonym',
        'genotype',
        'systematic name',
        'mutant type',
        'mutagenesis method',
        'public note',
      ],
      term_ontologies: [
        'Dicty Phenotypes',
        'Dicty Environment',
        'Dictyostelium Assay',
        'dicty_strain_characteristics',
      ],
    })
    .toArray();

  return res[0] || { tags: {}, graphs: {} };
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

function insertAnnotation(termDoc, entryId, value, opts) {
  var options = opts || {};
  var ann = db._collection(CALIB.annotation).insert({
    value: value,
    editable_value: options.editableValue !== undefined ? options.editableValue : value,
    created_by: 'siddbasu@gmail.com',
    entry_id: entryId,
    rank: options.rank || 0,
    is_obsolete: options.isObsolete === true,
    version: 1,
    created_at: new Date().toISOString(),
  });

  db._collection(CALIB.edge).insert({
    _from: ann._id,
    _to: termDoc._id,
  });

  return ann;
}

function insertFixtures(terms) {
  // Probe 1, 8, 14: synonym gammaS13
  insertAnnotation(terms['dicty_annotation:synonym'], 'DBS0236125', 'gammaS13');

  // Probe 2: genotype whole + interior allele prefix
  insertAnnotation(
    terms['dicty_annotation:genotype'],
    'DBS0236126',
    'axeA1, axeB1, sadA-[sadA-KO]'
  );

  // Probe 3: systematic_name, genetic_modification, mutagenesis_method
  insertAnnotation(
    terms['dicty_annotation:systematic name'],
    'DBS0236130',
    'AX4-sys'
  );
  insertAnnotation(
    terms['dicty_annotation:mutant type'],
    'DBS0236131',
    'endogenous insertion'
  );
  insertAnnotation(
    terms['Dd Mutagenesis Method:mutagenesis method'],
    'DBS0236132',
    'Restriction Enzyme-Mediated Integration'
  );

  // Probe 4, 6: phenotype, environment, assay, characteristic
  insertAnnotation(terms['phenotype:aberrant'], 'DBS0236140', 'aberrant fruiting body morphogenesis');
  insertAnnotation(terms['environment:suspension'], 'DBS0236141', 'in suspension');
  insertAnnotation(terms['assay:confocal'], 'DBS0236142', 'confocal microscopy');
  insertAnnotation(terms['characteristic:axenic'], 'DBS0236143', 'axenic');

  // Probe 5: public_note phrase vs token ranking
  insertAnnotation(
    terms['dicty_annotation:public note'],
    'DBS0236150',
    'mutant forms culminants under starvation'
  );
  insertAnnotation(
    terms['dicty_annotation:public note'],
    'DBS0236151',
    'culminants appear in mutant forms'
  );

  // Probe 7: 4-band ordering fixtures across bands
  insertAnnotation(
    terms['dicty_annotation:synonym'],
    'DBS0236181',
    'slug migration-A'
  );
  insertAnnotation(
    terms['dicty_annotation:public note'],
    'DBS0236182',
    'aberrant slug migration under light'
  );
  insertAnnotation(
    terms['dicty_annotation:public note'],
    'DBS0236183',
    'migration impaired during slug development'
  );
  insertAnnotation(
    terms['dicty_annotation:synonym'],
    'DBS0236184',
    'slug migratn'
  );

  // Probe 9: editable_value exclusion
  insertAnnotation(
    terms['dicty_annotation:public note'],
    'DBS0236160',
    'normal note',
    { editableValue: 'slateonlymarker' }
  );

  // Probe 10: obsolete annotation & deprecated cvterm
  insertAnnotation(
    terms['dicty_annotation:synonym'],
    'DBS0236128',
    'oldalias',
    { isObsolete: true }
  );
  insertAnnotation(
    terms['phenotype:obsolete'],
    'DBS0236161',
    'obsoletephenoterm'
  );

  // Probe 11: non-strain entries and unsearched tags under @limit = 1
  for (var g = 0; g < 10; g++) {
    insertAnnotation(
      terms['dicty_annotation:synonym'],
      'DDB_G028642' + g,
      'xyloseMutant'
    );
    insertAnnotation(
      terms['dicty_annotation:synonym'],
      'DBP028642' + g,
      'xyloseMutant'
    );
    insertAnnotation(
      terms['dicty_annotation:description'],
      'DBS023620' + g,
      'xyloseMutant'
    );
  }
  insertAnnotation(
    terms['dicty_annotation:synonym'],
    'DBS0236999',
    'xyloseMutant'
  );

  // Probe 12: high-fan-out ontology term under @limit = 50 (60 strains)
  for (var h = 0; h < 60; h++) {
    var numStr = ('000' + h).slice(-3);
    insertAnnotation(
      terms['phenotype:popular'],
      'DBS0239' + numStr,
      'popularpheno phenotype'
    );
  }

  // Probe 13: cross-collection deduplication by entry_id
  insertAnnotation(
    terms['dicty_annotation:synonym'],
    'DBS0236170',
    'cordaxStrain'
  );
  insertAnnotation(
    terms['phenotype:cordax'],
    'DBS0236170',
    'aberrant cordax development'
  );

  // Filler to reach ~1,000 documents
  var rand = makeRandom(20261015);
  var fillerTags = [
    terms['dicty_annotation:synonym'],
    terms['dicty_annotation:genotype'],
    terms['dicty_annotation:public note'],
  ];
  var currentCount = db._collection(CALIB.annotation).count();
  var needed = CALIB.fixtureCount - currentCount;
  for (var i = 0; i < needed; i++) {
    var tagDoc = fillerTags[Math.floor(rand() * fillerTags.length)];
    var entry =
      rand() < 0.5
        ? 'DBS02' + (300000 + Math.floor(rand() * 90000))
        : 'DDB_G0' + (300000 + Math.floor(rand() * 900000));
    var val = 'zzfill' + i + ' filler' + (rand() < 0.5 ? ' allele' : ' term');
    insertAnnotation(tagDoc, entry, val, { rank: Math.floor(rand() * 3) });
  }
}

// ------------------------------------------------------------- query building

var annoBranchTpl = [
  'LET %s = (',
  '  FOR d IN ' + CALIB.view,
  '    SEARCH d.is_obsolete == false',
  '      AND ANALYZER(STARTS_WITH(d.entry_id, "dbs"), "' + CALIB.norm + '")',
  '      AND %s',
  '    LET has_tag = FIRST(',
  '      FOR cvt, e IN 1..1 OUTBOUND d GRAPH @anno_cvterm_graph',
  '        FILTER e._to == @%s AND cvt.deprecated == false',
  '        RETURN true',
  '    )',
  '    FILTER has_tag == true',
  '    SORT BM25(d) DESC, d.entry_id ASC, d._key ASC',
  '    LIMIT @limit',
  '    RETURN {',
  '      id: d.entry_id,',
  '      f: %s,',
  '      v: NOT_NULL(d.value, ""),',
  '      s: %s',
  '    }',
  ')',
].join('\n');

var termBranchTpl = [
  'LET %s = (',
  '  FOR d IN ' + CALIB.view,
  '    SEARCH d.deprecated == false',
  '      AND %s',
  '      AND d.graph_id == @%s',
  '    FOR ann IN 1..1 INBOUND d GRAPH @anno_cvterm_graph',
  '      FILTER ann.is_obsolete == false',
  '        AND STARTS_WITH(LOWER(ann.entry_id), "dbs")',
  '      SORT BM25(d) DESC, ann.entry_id ASC, ann._key ASC',
  '      LIMIT @limit',
  '      RETURN {',
  '        id: ann.entry_id,',
  '        f: %s,',
  '        v: NOT_NULL(d.label, ""),',
  '        s: %s',
  '      }',
  ')',
].join('\n');

var mergeTpl = [
  'LET hits = FLATTEN([%s])',
  'LET best = (',
  '  FOR x IN hits',
  '    COLLECT eid = x.id INTO grp = x',
  '    LET top = FIRST(',
  '      FOR m IN grp',
  '        SORT m.s DESC, m.f ASC, m.v ASC',
  '        RETURN m',
  '    )',
  '    RETURN top',
  ')',
  'FOR x IN best',
  '  SORT x.s DESC, x.id ASC',
  '  LIMIT @limit',
  '  RETURN {',
  '    id: x.id,',
  '    f: x.f,',
  '    v: x.v,',
  '    s: x.s',
  '  }',
].join('\n');

var prefixFuzzyFields = [
  { label: 'synonym', coll: 'anno', bind: 'synonym_term_id', isGeno: false },
  { label: 'genotype', coll: 'anno', bind: 'genotype_term_id', isGeno: true },
  { label: 'systematic_name', coll: 'anno', bind: 'sysname_term_id', isGeno: false },
  { label: 'genetic_modification', coll: 'anno', bind: 'muttype_term_id', isGeno: false },
  { label: 'mutagenesis_method', coll: 'anno', bind: 'mutmethod_term_id', isGeno: false },
  { label: 'phenotype', coll: 'term', bind: 'phenotype_graph_id', isGeno: false },
  { label: 'environment', coll: 'term', bind: 'env_graph_id', isGeno: false },
  { label: 'assay', coll: 'term', bind: 'assay_graph_id', isGeno: false },
  { label: 'characteristic', coll: 'term', bind: 'char_graph_id', isGeno: false },
];

var textFields = [
  { label: 'public_note', coll: 'anno', bind: 'pubnote_term_id', isGeno: false },
  { label: 'phenotype', coll: 'term', bind: 'phenotype_graph_id', isGeno: false },
  { label: 'environment', coll: 'term', bind: 'env_graph_id', isGeno: false },
  { label: 'assay', coll: 'term', bind: 'assay_graph_id', isGeno: false },
  { label: 'characteristic', coll: 'term', bind: 'char_graph_id', isGeno: false },
];

function buildQuery() {
  var branches = [];
  var branchNames = [];

  // Set 1: Prefix p0..p8
  prefixFuzzyFields.forEach(function (f, i) {
    var name = 'p' + i;
    branchNames.push(name);
    var searchExpr;
    if (f.isGeno) {
      searchExpr =
        '(ANALYZER(STARTS_WITH(d.value, @q), "' +
        CALIB.norm +
        '") OR ANALYZER(STARTS_WITH(d.value, @q), "' +
        CALIB.delim +
        '"))';
    } else if (f.coll === 'anno') {
      searchExpr =
        'ANALYZER(STARTS_WITH(d.value, @q), "' + CALIB.norm + '")';
    } else {
      searchExpr =
        'ANALYZER(STARTS_WITH(d.label, @q), "' + CALIB.norm + '")';
    }
    var tpl = f.coll === 'anno' ? annoBranchTpl : termBranchTpl;
    branches.push(
      format(tpl, name, searchExpr, f.bind, JSON.stringify(f.label), '1000 + BM25(d)')
    );
  });

  // Set 2: Fuzzy n0..n8
  prefixFuzzyFields.forEach(function (f, i) {
    var name = 'n' + i;
    branchNames.push(name);
    var attr = f.coll === 'anno' ? 'value' : 'label';
    var searchExpr =
      'NGRAM_MATCH(d.' + attr + ', @q, @th, "' + CALIB.ngram + '")';
    var tpl = f.coll === 'anno' ? annoBranchTpl : termBranchTpl;
    branches.push(
      format(tpl, name, searchExpr, f.bind, JSON.stringify(f.label), 'BM25(d)')
    );
  });

  // Set 3: Token t0..t4
  textFields.forEach(function (f, i) {
    var name = 't' + i;
    branchNames.push(name);
    var attr = f.coll === 'anno' ? 'value' : 'label';
    var searchExpr =
      'ANALYZER(d.' + attr + ' IN TOKENS(@q, "' + CALIB.text + '"), "' + CALIB.text + '")';
    var tpl = f.coll === 'anno' ? annoBranchTpl : termBranchTpl;
    branches.push(
      format(tpl, name, searchExpr, f.bind, JSON.stringify(f.label), '250 + BM25(d)')
    );
  });

  // Set 4: Phrase h0..h4
  textFields.forEach(function (f, i) {
    var name = 'h' + i;
    branchNames.push(name);
    var attr = f.coll === 'anno' ? 'value' : 'label';
    var searchExpr =
      'PHRASE(d.' + attr + ', @q, "' + CALIB.text + '")';
    var tpl = f.coll === 'anno' ? annoBranchTpl : termBranchTpl;
    branches.push(
      format(tpl, name, searchExpr, f.bind, JSON.stringify(f.label), '500 + BM25(d)')
    );
  });

  return branches.join('\n') + '\n' + format(mergeTpl, branchNames.join(', '));
}

function makeBindVars(targets, text, limit, threshold, overrides) {
  var t = targets.tags || {};
  var g = targets.graphs || {};
  var vars = {
    q: text,
    th: threshold === undefined ? CALIB.ngramThreshold : threshold,
    limit: limit === undefined ? 50 : limit,
    anno_cvterm_graph: CALIB.graph,
    synonym_term_id: t['dicty_annotation:synonym'] || '',
    genotype_term_id: t['dicty_annotation:genotype'] || '',
    sysname_term_id: t['dicty_annotation:systematic name'] || '',
    muttype_term_id: t['dicty_annotation:mutant type'] || '',
    mutmethod_term_id: t['Dd Mutagenesis Method:mutagenesis method'] || '',
    pubnote_term_id: t['dicty_annotation:public note'] || '',
    phenotype_graph_id: g['Dicty Phenotypes'] || '',
    env_graph_id: g['Dicty Environment'] || '',
    assay_graph_id: g['Dictyostelium Assay'] || '',
    char_graph_id: g['dicty_strain_characteristics'] || '',
  };
  if (overrides) {
    Object.keys(overrides).forEach(function (k) {
      vars[k] = overrides[k];
    });
  }
  return vars;
}

function runQuery(query, targets, text, limit, threshold, overrides) {
  var b = makeBindVars(targets, text, limit, threshold, overrides);
  return db._query(query, b).toArray();
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

// ------------------------------------------------------------------- main

function main() {
  dropDisposableDatabase();
  createDisposableDatabase();
  createAnalyzers();
  var terms = createCollections();
  createView();
  insertFixtures(terms);

  var targets = resolveTargets();
  results.targets = targets;

  var query = buildQuery();
  results.query = query;

  // Wait for the view index commit
  waitForCommit(function () {
    return runQuery(query, targets, 'gammas', 5);
  });
  waitForCommit(function () {
    return runQuery(query, targets, 'ax4', 5);
  });
  waitForCommit(function () {
    return runQuery(query, targets, 'popularpheno', 5);
  });

  var observedBM25 = [];

  function recordBM25(rows) {
    rows.forEach(function (r) {
      var score = r.s || 0;
      var raw = score;
      if (raw >= 1000) raw -= 1000;
      else if (raw >= 500) raw -= 500;
      else if (raw >= 250) raw -= 250;
      observedBM25.push(raw);
    });
  }

  // Probe 1: synonym value prefix
  probe('1_synonym_value_prefix', function () {
    var rows = runQuery(query, targets, 'gammas', 10);
    recordBM25(rows);
    return {
      rows: rows,
      pass:
        rows.length === 1 &&
        rows[0].id === 'DBS0236125' &&
        rows[0].f === 'synonym' &&
        rows[0].v === 'gammaS13' &&
        rows[0].s >= 1000,
    };
  });

  // Probe 2: genotype whole + interior allele prefix
  probe('2_genotype_allele_prefix', function () {
    var rowsWhole = runQuery(query, targets, 'axea', 10);
    var rowsInterior = runQuery(query, targets, 'sada', 10);
    recordBM25(rowsWhole);
    recordBM25(rowsInterior);
    var passWhole = rowsWhole.some(function (r) {
      return r.id === 'DBS0236126' && r.f === 'genotype' && r.s >= 1000;
    });
    var passInterior = rowsInterior.some(function (r) {
      return r.id === 'DBS0236126' && r.f === 'genotype' && r.s >= 1000;
    });
    return {
      rowsWholeCount: rowsWhole.length,
      rowsInteriorCount: rowsInterior.length,
      pass: passWhole && passInterior,
    };
  });

  // Probe 3: systematic_name, genetic_modification, mutagenesis_method prefixes
  probe('3_anno_other_prefixes', function () {
    var rSys = runQuery(query, targets, 'ax4', 10);
    var rMod = runQuery(query, targets, 'endogenous', 10);
    var rMut = runQuery(query, targets, 'restriction', 10);
    recordBM25(rSys);
    recordBM25(rMod);
    recordBM25(rMut);
    var topSys = rSys[0];
    var topMod = rMod[0];
    var topMut = rMut[0];
    var passSys =
      topSys &&
      topSys.id === 'DBS0236130' &&
      topSys.f === 'systematic_name' &&
      topSys.s >= 1000 &&
      rSys.filter(function (r) { return r.s >= 1000; }).length === 1;
    var passMod =
      topMod &&
      topMod.id === 'DBS0236131' &&
      topMod.f === 'genetic_modification' &&
      topMod.s >= 1000 &&
      rMod.filter(function (r) { return r.s >= 1000; }).length === 1;
    var passMut =
      topMut &&
      topMut.id === 'DBS0236132' &&
      topMut.f === 'mutagenesis_method' &&
      topMut.s >= 1000 &&
      rMut.filter(function (r) { return r.s >= 1000; }).length === 1;
    return {
      pass: passSys && passMod && passMut,
      rSysTop: topSys,
      rModTop: topMod,
      rMutTop: topMut,
    };
  });

  // Probe 4: phenotype, environment, assay, characteristic term prefixes
  probe('4_cvterm_prefixes', function () {
    var rPheno = runQuery(query, targets, 'aberrant', 10);
    var rEnv = runQuery(query, targets, 'in suspension', 10);
    var rAssay = runQuery(query, targets, 'confocal', 10);
    var rChar = runQuery(query, targets, 'axenic', 10);
    recordBM25(rPheno);
    recordBM25(rEnv);
    recordBM25(rAssay);
    recordBM25(rChar);
    var passPheno = rPheno.some(function (r) {
      return r.id === 'DBS0236140' && r.f === 'phenotype' && r.s >= 1000;
    });
    var passEnv = rEnv.some(function (r) {
      return r.id === 'DBS0236141' && r.f === 'environment' && r.s >= 1000;
    });
    var passAssay = rAssay.some(function (r) {
      return r.id === 'DBS0236142' && r.f === 'assay' && r.s >= 1000;
    });
    var passChar = rChar.some(function (r) {
      return r.id === 'DBS0236143' && r.f === 'characteristic' && r.s >= 1000;
    });
    return {
      pass: passPheno && passEnv && passAssay && passChar,
      rPheno: rPheno,
      rEnv: rEnv,
      rAssay: rAssay,
      rChar: rChar,
    };
  });

  // Probe 5: public_note phrase vs token ranking
  probe('5_public_note_phrase_vs_token', function () {
    var rows = runQuery(query, targets, 'forms culminants', 10);
    recordBM25(rows);
    var pass =
      rows.length === 2 &&
      rows[0].id === 'DBS0236150' &&
      rows[0].f === 'public_note' &&
      rows[0].s >= 500 &&
      rows[1].id === 'DBS0236151' &&
      rows[1].f === 'public_note' &&
      rows[1].s >= 250 &&
      rows[1].s < 500;
    return {
      rows: rows,
      pass: pass,
    };
  });

  // Probe 6: ontology term interior phrase & token
  probe('6_ontology_term_interior_phrase_and_token', function () {
    var rPhrase = runQuery(query, targets, 'fruiting body', 10);
    var rToken = runQuery(query, targets, 'morphogenesis', 10);
    recordBM25(rPhrase);
    recordBM25(rToken);
    var matchPhrase = rPhrase.filter(function (r) {
      return r.id === 'DBS0236140';
    });
    var matchToken = rToken.filter(function (r) {
      return r.id === 'DBS0236140';
    });
    var passPhrase =
      matchPhrase.length === 1 &&
      matchPhrase[0].f === 'phenotype' &&
      matchPhrase[0].s >= 500;
    var passToken =
      matchToken.length === 1 &&
      matchToken[0].f === 'phenotype' &&
      matchToken[0].s >= 250;
    return {
      pass: passPhrase && passToken,
      matchPhrase: matchPhrase,
      matchToken: matchToken,
    };
  });

  // Probe 7: 4-band ordering (prefix > phrase > token > fuzzy)
  probe('7_four_band_ordering', function () {
    var rows = runQuery(query, targets, 'slug migration', 10);
    recordBM25(rows);
    var pass =
      rows.length === 4 &&
      rows[0].s >= 1000 &&
      rows[1].s >= 500 &&
      rows[1].s < 1000 &&
      rows[2].s >= 250 &&
      rows[2].s < 500 &&
      rows[3].s < 250 &&
      rows[0].s > rows[1].s &&
      rows[1].s > rows[2].s &&
      rows[2].s > rows[3].s;
    return {
      rows: rows,
      pass: pass,
    };
  });

  // Probe 8: fuzzy typo on value and cvterm
  probe('8_fuzzy_typo_value_and_cvterm', function () {
    var rVal = runQuery(query, targets, 'gammas14', 10);
    var rTerm = runQuery(query, targets, 'axenik', 10);
    recordBM25(rVal);
    recordBM25(rTerm);
    var passVal =
      rVal.length === 1 &&
      rVal[0].id === 'DBS0236125' &&
      rVal[0].f === 'synonym' &&
      rVal[0].s > 0 &&
      rVal[0].s < 250;
    var passTerm =
      rTerm.length === 1 &&
      rTerm[0].id === 'DBS0236143' &&
      rTerm[0].f === 'characteristic' &&
      rTerm[0].s > 0 &&
      rTerm[0].s < 250;
    return {
      pass: passVal && passTerm,
      rVal: rVal,
      rTerm: rTerm,
    };
  });

  // Probe 9: editable_value exclusion
  probe('9_editable_value_exclusion', function () {
    var rows = runQuery(query, targets, 'slateonlymarker', 10);
    recordBM25(rows);
    return {
      rows: rows,
      pass: rows.length === 0,
    };
  });

  // Probe 10: obsolete annotation & deprecated cvterm exclusion
  probe('10_obsolete_and_deprecated_exclusion', function () {
    var rObs = runQuery(query, targets, 'oldalias', 10);
    var rDep = runQuery(query, targets, 'obsoletephenoterm', 10);
    recordBM25(rObs);
    recordBM25(rDep);
    return {
      pass: rObs.length === 0 && rDep.length === 0,
      rObsCount: rObs.length,
      rDepCount: rDep.length,
    };
  });

  // Probe 11: non-strain and unsearched tag exclusion under @limit = 1
  probe('11_non_strain_and_unsearched_tag_exclusion', function () {
    var rows = runQuery(query, targets, 'xylose', 1);
    recordBM25(rows);
    return {
      rows: rows,
      pass:
        rows.length === 1 &&
        rows[0].id === 'DBS0236999' &&
        rows[0].f === 'synonym' &&
        rows[0].s >= 1000,
    };
  });

  // Probe 12: high-fan-out ontology term under @limit = 50
  probe('12_high_fan_out_term_order_and_limit', function () {
    var rows = runQuery(query, targets, 'popularpheno', 50);
    recordBM25(rows);
    var passCount = rows.length === 50;
    var isSorted = true;
    for (var i = 1; i < rows.length; i++) {
      if (rows[i].id < rows[i - 1].id) {
        isSorted = false;
        break;
      }
    }
    return {
      pass: passCount && isSorted,
      count: rows.length,
      firstId: rows[0] ? rows[0].id : null,
      lastId: rows[rows.length - 1] ? rows[rows.length - 1].id : null,
      isSorted: isSorted,
    };
  });

  // Probe 13: cross-collection deduplication by entry_id
  probe('13_cross_collection_deduplication', function () {
    var rows = runQuery(query, targets, 'cordax', 50);
    recordBM25(rows);
    var mine = rows.filter(function (r) {
      return r.id === 'DBS0236170';
    });
    return {
      rows: rows,
      pass:
        mine.length === 1 &&
        mine[0].f === 'synonym' &&
        mine[0].s >= 1000,
    };
  });

  // Probe 14: unloaded ontology resilience
  probe('14_unloaded_ontology_resilience', function () {
    var rows = runQuery(query, targets, 'gammas', 10, undefined, {
      env_graph_id: '',
    });
    recordBM25(rows);
    return {
      rows: rows,
      pass:
        rows.length === 1 &&
        rows[0].id === 'DBS0236125' &&
        rows[0].f === 'synonym' &&
        rows[0].s >= 1000,
    };
  });

  // Probe 15: TOKENS("the and", "text_en") stopword & BM25(d) < 250 check
  probe('15_stopword_and_bm25_band_width_check', function () {
    var tokensRes = db
      ._query('RETURN TOKENS("the and", "text_en")')
      .toArray();
    var tokens = tokensRes[0] || [];
    var maxBM25 = observedBM25.reduce(function (m, val) {
      return val > m ? val : m;
    }, 0);
    return {
      tokens: tokens,
      tokensNonEmpty: tokens.length > 0,
      maxBM25: maxBM25,
      pass: tokens.length > 0 && maxBM25 < 250,
    };
  });

  // --------------------------------------------------------------- EXPLAIN

  var explainResponse = db._connection.POST(
    '/_api/explain',
    JSON.stringify({
      query: query,
      bindVars: makeBindVars(targets, 'gammas', 50),
    }),
    { 'content-type': 'application/json' }
  );

  var explained =
    typeof explainResponse === 'string'
      ? JSON.parse(explainResponse)
      : explainResponse;
  var counts = {};
  (explained.plan ? explained.plan.nodes : []).forEach(function (node) {
    counts[node.type] = (counts[node.type] || 0) + 1;
  });

  results.explain = {
    nodes: counts,
    enumerateViewNode: counts.EnumerateViewNode || 0,
    enumerateCollectionNode: counts.EnumerateCollectionNode || 0,
    traversalNode: counts.TraversalNode || 0,
    pass:
      (counts.EnumerateViewNode || 0) === 28 &&
      (counts.EnumerateCollectionNode || 0) === 0 &&
      (counts.TraversalNode || 0) === 28,
  };

  // --------------------------------------------------------------- latency

  var timings = [];
  var lastRows = [];
  for (var run = 0; run < 20; run++) {
    var started = Date.now();
    lastRows = runQuery(query, targets, 'gammas', 50);
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

  var envKeep = require('internal').env && require('internal').env.CALIB_KEEP;
  if (!(envKeep === '1')) {
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
  if (failures.length > 0) {
    throw new Error('calibration failed: ' + failures.join(', '));
  }
  log('all calibration gates passed');
}

main();
